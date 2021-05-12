package redis

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/go-redsync/redsync/v4"
	redsyncredis "github.com/go-redsync/redsync/v4/redis/redigo"
	"github.com/gomodule/redigo/redis"

	"github.com/RichardKnop/machinery/v1/brokers/errs"
	"github.com/RichardKnop/machinery/v1/brokers/iface"
	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
)

const defaultRedisDelayedTasksKey = "delayed_tasks"

const (
	luaPumpQueueScript = `
local zset_key = KEYS[1]
local default_queue = KEYS[2]
local now = ARGV[1]
local limit = ARGV[2]

local expiredMembers = redis.call("ZRANGEBYSCORE", zset_key, 0, now, "LIMIT", 0, limit)

if #expiredMembers == 0 then
	return 0
end

for _,v in ipairs(expiredMembers) do
	-- move to ready queue
	local routingKey, job_id = struct.unpack("Hc0Hc0", v)
	local job_key = table.concat({"job", job_id}, "_")
	local job = redis.call("GET", job_key)
	if job then
		redis.call("RPUSH", routingKey, job)
		redis.call("del", job_key)
	end
end
redis.call("ZREM", zset_key, unpack(expiredMembers))
return #expiredMembers
`
	luaAddQueueScript = `
local zset_key = KEYS[1]
local score = ARGV[1]
local member = ARGV[2]
local job_id = ARGV[3]
local job = ARGV[4]

local zset_ret = redis.call("ZADD", zset_key, score, member)
if zset_ret == 0 then
	return ""
end

local set_ret = redis.call("SET", table.concat({"job", job_id}, "_"), job)
return set_ret
`
	luaRemoveQueueScript= `
local zset_key = KEYS[1]
local member = ARGV[1]
local job_id = ARGV[2]

local zset_ret = redis.call("ZREM", zset_key, member)
if zset_ret == 0 then
	return zset_ret
end

local del_ret = redis.call("DEL", table.concat({"job", job_id}, "_"))
return del_ret
`
)

// Broker represents a Redis broker
type Broker struct {
	common.Broker
	common.RedisConnector
	host         string
	password     string
	db           int
	pool         *redis.Pool
	consumingWG  sync.WaitGroup // wait group to make sure whole consumption completes
	processingWG sync.WaitGroup // use wait group to make sure task processing completes
	delayedWG    sync.WaitGroup
	// If set, path to a socket file overrides hostname
	socketPath           string
	redsync              *redsync.Redsync
	redisOnce            sync.Once
	redisDelayedTasksKey string
}

// New creates new Broker instance
func New(cnf *config.Config, host, password, socketPath string, db int) iface.Broker {
	b := &Broker{Broker: common.NewBroker(cnf)}
	b.host = host
	b.db = db
	b.password = password
	b.socketPath = socketPath

	if cnf.Redis != nil && cnf.Redis.DelayedTasksKey != "" {
		b.redisDelayedTasksKey = cnf.Redis.DelayedTasksKey
	} else {
		b.redisDelayedTasksKey = defaultRedisDelayedTasksKey
	}

	return b
}

// StartConsuming enters a loop and waits for incoming messages
func (b *Broker) StartConsuming(consumerTag string, concurrency int, taskProcessor iface.TaskProcessor) (bool, error) {
	b.consumingWG.Add(1)
	defer b.consumingWG.Done()

	if concurrency < 1 {
		concurrency = runtime.NumCPU() * 2
	}

	b.Broker.StartConsuming(consumerTag, concurrency, taskProcessor)

	conn := b.open()
	defer conn.Close()

	// Ping the server to make sure connection is live
	_, err := conn.Do("PING")
	if err != nil {
		b.GetRetryFunc()(b.GetRetryStopChan())

		// Return err if retry is still true.
		// If retry is false, broker.StopConsuming() has been called and
		// therefore Redis might have been stopped. Return nil exit
		// StartConsuming()
		if b.GetRetry() {
			return b.GetRetry(), err
		}
		return b.GetRetry(), errs.ErrConsumerStopped
	}

	// Channel to which we will push tasks ready for processing by worker
	deliveries := make(chan []byte, concurrency)
	pool := make(chan struct{}, concurrency)

	// initialize worker pool with maxWorkers workers
	for i := 0; i < concurrency; i++ {
		pool <- struct{}{}
	}

	// A receiving goroutine keeps popping messages from the queue by BLPOP
	// If the message is valid and can be unmarshaled into a proper structure
	// we send it to the deliveries channel
	go func() {

		log.INFO.Print("[*] Waiting for messages. To exit press CTRL+C")

		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.GetStopChan():
				close(deliveries)
				return
			case <-pool:
				select {
				case <-b.GetStopChan():
					close(deliveries)
					return
				default:
				}

				if taskProcessor.PreConsumeHandler() {
					task, _ := b.nextTask(getQueue(b.GetConfig(), taskProcessor))
					//TODO: should this error be ignored?
					if len(task) > 0 {
						deliveries <- task
					}
				}

				pool <- struct{}{}
			}
		}
	}()

	// A goroutine to watch for delayed tasks and push them to deliveries
	// channel for consumption by the worker
	b.delayedWG.Add(1)
	go func() {
		defer b.delayedWG.Done()

		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.GetStopChan():
				return
			default:
				_, err := b.nextDelayedTask(b.redisDelayedTasksKey)
				if err != nil {
					continue
				}

				//for _, task := range taskArray {
				//	signature := new(tasks.Signature)
				//	decoder := json.NewDecoder(bytes.NewReader(task))
				//	decoder.UseNumber()
				//	if err := decoder.Decode(signature); err != nil {
				//		log.ERROR.Print(errs.NewErrCouldNotUnmarshalTaskSignature(task, err))
				//	}
				//
				//	if err := b.Publish(context.Background(), signature); err != nil {
				//		log.ERROR.Print(err)
				//	}
				//}
			}
		}
	}()

	if err := b.consume(deliveries, concurrency, taskProcessor); err != nil {
		return b.GetRetry(), err
	}

	// Waiting for any tasks being processed to finish
	b.processingWG.Wait()

	return b.GetRetry(), nil
}

// StopConsuming quits the loop
func (b *Broker) StopConsuming() {
	b.Broker.StopConsuming()
	// Waiting for the delayed tasks goroutine to have stopped
	b.delayedWG.Wait()
	// Waiting for consumption to finish
	b.consumingWG.Wait()
	// Wait for currently processing tasks to finish as well.
	b.processingWG.Wait()

	if b.pool != nil {
		b.pool.Close()
	}
}

func (b *Broker) UnPublish(ctx context.Context, jobId, queue string) error {
	conn := b.open()
	defer conn.Close()

	if queue == "" {
		queue = b.GetConfig().DefaultQueue
	}

	// struct-pack the data in the format `Hc0Hc0`:
	//   {taskId len}{taskId}{task len}{task}
	// length are 2-byte uint16 in little-endian
	taskIdLen := len(jobId)
	routingKeyLen := len(queue)
	msgBytes := make([]byte, 0)
	buffer := bytes.NewBuffer(msgBytes)
	_ = binary.Write(buffer, binary.LittleEndian, uint16(routingKeyLen))
	_ = binary.Write(buffer, binary.LittleEndian, []byte(queue))
	_ = binary.Write(buffer, binary.LittleEndian, uint16(taskIdLen))
	_ = binary.Write(buffer, binary.LittleEndian, []byte(jobId))
	script := redis.NewScript(1, luaRemoveQueueScript)
	val, scriptErr := script.Do(conn, b.redisDelayedTasksKey, buffer, jobId)
	if scriptErr != nil {
		return scriptErr
	}
	if val.(int64) == 0 {
		return fmt.Errorf("remove task failed")
	}

	return nil
}

// Publish places a new message on the default queue
func (b *Broker) Publish(ctx context.Context, signature *tasks.Signature) error {
	// Adjust routing key (this decides which queue the message will be published to)
	b.Broker.AdjustRoutingKey(signature)

	msg, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	conn := b.open()
	defer conn.Close()

	// Check the ETA signature field, if it is set and it is in the future,
	// delay the task
	if signature.ETA != nil {
		now := time.Now().UTC()

		if signature.ETA.After(now) {
			score := signature.ETA.UnixNano()

			// struct-pack the data in the format `Hc0Hc0`:
			//   {taskId len}{taskId}{task len}{task}
			// length are 2-byte uint16 in little-endian
			taskIdLen := len(signature.UUID)
			routingKeyLen := len(signature.RoutingKey)
			msgBytes := make([]byte, 0)
			buffer := bytes.NewBuffer(msgBytes)
			_ = binary.Write(buffer, binary.LittleEndian, uint16(routingKeyLen))
			_ = binary.Write(buffer, binary.LittleEndian, []byte(signature.RoutingKey))
			_ = binary.Write(buffer, binary.LittleEndian, uint16(taskIdLen))
			_ = binary.Write(buffer, binary.LittleEndian, []byte(signature.UUID))
			script := redis.NewScript(1, luaAddQueueScript)
			val, scriptErr := script.Do(conn, b.redisDelayedTasksKey, score, buffer, signature.UUID, msg)
			if scriptErr != nil {
				return scriptErr
			}
			if val.(string) != "OK" {
				return fmt.Errorf("eval luaAddQueueScript result 0")
			}
			//_, err = conn.Do("ZADD", b.redisDelayedTasksKey, score, buffer)
			//return err
			return nil
		}
	}

	_, err = conn.Do("RPUSH", signature.RoutingKey, msg)
	return err
}

// GetPendingTasks returns a slice of task signatures waiting in the queue
func (b *Broker) GetPendingTasks(queue string) ([]*tasks.Signature, error) {
	conn := b.open()
	defer conn.Close()

	if queue == "" {
		queue = b.GetConfig().DefaultQueue
	}
	dataBytes, err := conn.Do("LRANGE", queue, 0, -1)
	if err != nil {
		return nil, err
	}
	results, err := redis.ByteSlices(dataBytes, err)
	if err != nil {
		return nil, err
	}

	taskSignatures := make([]*tasks.Signature, len(results))
	for i, result := range results {
		signature := new(tasks.Signature)
		decoder := json.NewDecoder(bytes.NewReader(result))
		decoder.UseNumber()
		if err := decoder.Decode(signature); err != nil {
			return nil, err
		}
		taskSignatures[i] = signature
	}
	return taskSignatures, nil
}

// GetDelayedTasks returns a slice of task signatures that are scheduled, but not yet in the queue
func (b *Broker) GetDelayedTasks() ([]*tasks.Signature, error) {
	conn := b.open()
	defer conn.Close()

	dataBytes, err := conn.Do("ZRANGE", b.redisDelayedTasksKey, 0, -1)
	if err != nil {
		return nil, err
	}
	results, err := redis.ByteSlices(dataBytes, err)
	if err != nil {
		return nil, err
	}

	taskSignatures := make([]*tasks.Signature, len(results))
	for i, result := range results {
		signature := new(tasks.Signature)
		decoder := json.NewDecoder(bytes.NewReader(result))
		decoder.UseNumber()
		if err := decoder.Decode(signature); err != nil {
			return nil, err
		}
		taskSignatures[i] = signature
	}
	return taskSignatures, nil
}

// consume takes delivered messages from the channel and manages a worker pool
// to process tasks concurrently
func (b *Broker) consume(deliveries <-chan []byte, concurrency int, taskProcessor iface.TaskProcessor) error {
	errorsChan := make(chan error, concurrency*2)
	pool := make(chan struct{}, concurrency)

	// init pool for Worker tasks execution, as many slots as Worker concurrency param
	go func() {
		for i := 0; i < concurrency; i++ {
			pool <- struct{}{}
		}
	}()

	for {
		select {
		case err := <-errorsChan:
			return err
		case d, open := <-deliveries:
			if !open {
				return nil
			}
			if concurrency > 0 {
				// get execution slot from pool (blocks until one is available)
				select {
				case <-b.GetStopChan():
					b.requeueMessage(d, taskProcessor)
					continue
				case <-pool:
				}
			}

			b.processingWG.Add(1)

			// Consume the task inside a goroutine so multiple tasks
			// can be processed concurrently
			go func() {
				if err := b.consumeOne(d, taskProcessor); err != nil {
					errorsChan <- err
				}

				b.processingWG.Done()

				if concurrency > 0 {
					// give slot back to pool
					pool <- struct{}{}
				}
			}()
		}
	}
}

// consumeOne processes a single message using TaskProcessor
func (b *Broker) consumeOne(delivery []byte, taskProcessor iface.TaskProcessor) error {
	signature := new(tasks.Signature)
	decoder := json.NewDecoder(bytes.NewReader(delivery))
	decoder.UseNumber()
	if err := decoder.Decode(signature); err != nil {
		return errs.NewErrCouldNotUnmarshalTaskSignature(delivery, err)
	}

	// If the task is not registered, we requeue it,
	// there might be different workers for processing specific tasks
	if !b.IsTaskRegistered(signature.Name) {
		if signature.IgnoreWhenTaskNotRegistered {
			return nil
		}
		log.INFO.Printf("Task not registered with this worker. Requeuing message: %s", delivery)
		b.requeueMessage(delivery, taskProcessor)
		return nil
	}

	//log.DEBUG.Printf("Received new message: %s", delivery)

	return taskProcessor.Process(signature)
}

// nextTask pops next available task from the default queue
func (b *Broker) nextTask(queue string) (result []byte, err error) {
	conn := b.open()
	defer conn.Close()

	pollPeriodMilliseconds := 1000 // default poll period for normal tasks
	if b.GetConfig().Redis != nil {
		configuredPollPeriod := b.GetConfig().Redis.NormalTasksPollPeriod
		if configuredPollPeriod > 0 {
			pollPeriodMilliseconds = configuredPollPeriod
		}
	}
	pollPeriod := time.Duration(pollPeriodMilliseconds) * time.Millisecond

	// Issue 548: BLPOP expects an integer timeout expresses in seconds.
	// The call will if the value is a float. Convert to integer using
	// math.Ceil():
	//   math.Ceil(0.0) --> 0 (block indefinitely)
	//   math.Ceil(0.2) --> 1 (timeout after 1 second)
	pollPeriodSeconds := math.Ceil(pollPeriod.Seconds())

	items, err := redis.ByteSlices(conn.Do("BLPOP", queue, pollPeriodSeconds))
	if err != nil {
		return []byte{}, err
	}

	// items[0] - the name of the key where an element was popped
	// items[1] - the value of the popped element
	if len(items) != 2 {
		return []byte{}, redis.ErrNil
	}

	result = items[1]

	return result, nil
}

// nextDelayedTask pops a value from the ZSET key using WATCH/MULTI/EXEC commands.
// https://github.com/gomodule/redigo/blob/master/redis/zpop_example_test.go
func (b *Broker) nextDelayedTask(key string) (result [][]byte, err error) {
	conn := b.open()
	defer conn.Close()

	//defer func() {
	//	// Return connection to normal state on error.
	//	// https://redis.io/commands/discard
	//	// https://redis.io/commands/unwatch
	//	if err == redis.ErrNil {
	//		conn.Do("UNWATCH")
	//	} else if err != nil {
	//		conn.Do("DISCARD")
	//	}
	//}()

	//var (
	//	items [][]byte
	//	reply interface{}
	//)

	pollPeriod := 500 // default poll period for delayed tasks
	if b.GetConfig().Redis != nil {
		configuredPollPeriod := b.GetConfig().Redis.DelayedTasksPollPeriod
		// the default period is 0, which bombards redis with requests, despite
		// our intention of doing the opposite
		if configuredPollPeriod > 0 {
			pollPeriod = configuredPollPeriod
		}
	}

	for {
		// Space out queries to ZSET so we don't bombard redis
		// server with relentless ZRANGEBYSCOREs
		time.Sleep(time.Duration(pollPeriod) * time.Millisecond)
		//if _, err = conn.Do("WATCH", key); err != nil {
		//	return
		//}

		now := time.Now().UTC().UnixNano()

		limit := 100
		script := redis.NewScript(2, luaPumpQueueScript)
		val, scriptErr := script.Do(conn, key, b.GetConfig().DefaultQueue, now, limit)
		if scriptErr != nil {
			err = scriptErr
			return
		}
		if val.(int64) == int64(limit) {
			continue
		}

		break

		//// https://redis.io/commands/zrangebyscore
		//items, err = redis.ByteSlices(conn.Do(
		//	"ZRANGEBYSCORE",
		//	key,
		//	0,
		//	now,
		//	"LIMIT",
		//	0,
		//	100,
		//))
		//if err != nil {
		//	return
		//}
		//if len(items) == 0 {
		//	err = redis.ErrNil
		//	time.Sleep(500 * time.Millisecond)
		//	return
		//}
		//
		////_ = conn.Send("MULTI")
		////_ = conn.Send("ZREM", key, items[0])
		////reply, err = conn.Do("EXEC")
		//var args []interface{}
		//args = append(args, key)
		//for _, item := range items {
		//	args = append(args, item)
		//}
		//reply, err = conn.Do("ZREM", args...)
		//if err != nil {
		//	return
		//}
		//
		//if reply != nil {
		//	result = items
		//	break
		//}
	}

	time.Sleep(100 * time.Millisecond)
	return
}

// open returns or creates instance of Redis connection
func (b *Broker) open() redis.Conn {
	b.redisOnce.Do(func() {
		b.pool = b.NewPool(b.socketPath, b.host, b.password, b.db, b.GetConfig().Redis, b.GetConfig().TLSConfig)
		b.redsync = redsync.New(redsyncredis.NewPool(b.pool))
	})

	return b.pool.Get()
}

func getQueue(config *config.Config, taskProcessor iface.TaskProcessor) string {
	customQueue := taskProcessor.CustomQueue()
	if customQueue == "" {
		return config.DefaultQueue
	}
	return customQueue
}

func (b *Broker) requeueMessage(delivery []byte, taskProcessor iface.TaskProcessor) {
	conn := b.open()
	defer conn.Close()
	conn.Do("RPUSH", getQueue(b.GetConfig(), taskProcessor), delivery)
}
