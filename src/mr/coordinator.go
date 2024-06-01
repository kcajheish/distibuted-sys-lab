package mr

import (
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	Tasks            []*Task
	IdleQ            SafeHeap
	ProcessPQ        SafeHeap
	NumOfReduceTasks int
	NumOfMapTasks    int
	Partitions       SafeMap
	Counter          SafeCounter
}

const MAP = 0
const REDUCE = 1

const IDLE = 0
const IN_PROGRESS = 1
const COMPLETED = 2

var typeMap = map[int]string{
	MAP:    "map",
	REDUCE: "reduce",
}

var statusMap = map[int]string{
	IDLE:        "idle",
	IN_PROGRESS: "in-progress",
	COMPLETED:   "completed",
}

type Task struct {
	TaskNumber int
	Files      []string
	JobType    int
	Status     int
	UnixTime   int64
	index      int // position at the priority queue
}

func (c *Coordinator) pollTask() *Task {
	c.Counter.Lock()
	var task *Task
	for {
		task = c.IdleQ.Pop()
		if task == nil && c.Counter.Count < c.NumOfMapTasks {
			c.Counter.Wait()
		} else {
			break
		}
	}
	c.Counter.Unlock()
	return task
}

func (c *Coordinator) GetTask(args TaskArg, reply *TaskReply) error {
	task := c.pollTask()
	if task == nil {
		reply.JobType = "exit"
		return nil
	}
	task.UnixTime = time.Now().UnixMicro()
	task.Status = IN_PROGRESS
	c.ProcessPQ.Push(task)
	reply.Files = task.Files
	var jType string
	if task.JobType == MAP {
		jType = "map"
	} else {
		jType = "reduce"
	}
	reply.JobType = jType
	reply.TaskNumber = task.TaskNumber
	reply.NumOfReduceTasks = c.NumOfReduceTasks
	log.Printf("master %d assign %s task %d to worker", os.Getpid(), jType, task.TaskNumber)
	return nil
}

func (c *Coordinator) makeReduceTasks() {
	for p := 0; p < c.NumOfReduceTasks; p++ {
		files := c.Partitions.Read(p)
		task := &Task{
			TaskNumber: p + c.NumOfMapTasks,
			Files:      files,
			JobType:    REDUCE,
			Status:     IDLE,
			UnixTime:   time.Now().UnixMicro(),
		}
		c.IdleQ.Push(task)
		c.Tasks = append(c.Tasks, task)
	}
}

func (c *Coordinator) CompleteTask(args TaskCompleteArg, reply *TaskCompleteReply) error {
	log.Printf("master %d receives complete %s task %d", os.Getpid(), args.JobType, args.TaskNumber)
	task := c.Tasks[args.TaskNumber]
	if args.JobType == "map" {
		for _, file := range args.OutputFiles {
			words := strings.Split(file, "-")
			partitionNumber, err := strconv.Atoi(words[2]) // mr-<map number>-<partition number>
			if err != nil {
				log.Fatalf("can't convert %d to partition number", partitionNumber)
			}
			c.Partitions.Append(partitionNumber, file)
		}
		c.Counter.Lock()
		if task.Status == IN_PROGRESS {
			c.Counter.Count += 1
			c.ProcessPQ.Done(task)
			task.Status = COMPLETED
			if c.Counter.Count == c.NumOfMapTasks {
				log.Printf("master %d writes out all partitions from memory to files", os.Getpid())
				c.makeReduceTasks()
				log.Printf("master %d wake up all sleeping reduce worker", os.Getpid())
				c.Counter.Broadcast()
			}
		} else {
			log.Printf("master %d receives completed %s task %d currently in status %s", os.Getpid(), typeMap[task.JobType], task.TaskNumber, statusMap[task.Status])
		}
		c.Counter.Unlock()
	} else if args.JobType == "reduce" {
		c.Counter.Lock()
		if task.Status == IN_PROGRESS {
			task.Status = COMPLETED
			c.ProcessPQ.Done(task)
			c.Counter.Count += 1
		} else {
			log.Printf("master %d receives completed %s task %d currently in status %s", os.Getpid(), typeMap[task.JobType], task.TaskNumber, statusMap[task.Status])
		}
		c.Counter.Unlock()
	}
	reply.Status = "success"
	log.Printf("master %d marks %s task %d  as compelted", os.Getpid(), args.JobType, args.TaskNumber)
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.Counter.Lock()
	defer c.Counter.Unlock()
	return c.NumOfMapTasks+c.NumOfReduceTasks == c.Counter.Count
}

func (c *Coordinator) initTasks(files []string) {
	c.Tasks = make([]*Task, 0, c.NumOfMapTasks+c.NumOfReduceTasks)
	for i, file := range files {
		task := &Task{
			TaskNumber: i,
			Status:     IDLE,
			JobType:    MAP,
			Files:      []string{file},
			UnixTime:   time.Now().UnixMicro(), // unix time in micro seconds
		}
		c.Tasks = append(c.Tasks, task)
	}
}

func (c *Coordinator) initIdleQueue() {
	pq := make(PriorityQueue, 0, c.NumOfMapTasks+c.NumOfReduceTasks)
	c.IdleQ = NewPQ(&pq)
	for _, task := range c.Tasks {
		c.IdleQ.Push(task)
	}
}

func (c *Coordinator) initProcessQueue() {
	pq := make(PriorityQueue, 0, c.NumOfMapTasks+c.NumOfReduceTasks)
	c.ProcessPQ = NewPQ(&pq)
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	log.Printf("start master id=%d", os.Getpid())
	c := Coordinator{
		NumOfReduceTasks: nReduce,
		NumOfMapTasks:    len(files),
		Partitions: SafeMap{
			partitions: make(map[int][]string, 0),
		},
		Counter: SafeCounter{
			Count: 0,
			Cond:  sync.NewCond(&sync.Mutex{}),
		},
	}
	log.Println("init task queue")
	c.initTasks(files)
	log.Println("init idle queue")
	c.initIdleQueue()
	log.Println("init process queue")
	c.initProcessQueue()
	log.Println("run coordinator server")
	c.server()
	go func() {
		maxDuration := int64(math.Pow10(6) * 10) // 10 second in us
		for {
			now := time.Now().UnixMicro()
			if task := c.ProcessPQ.ExpireAndPop(now, maxDuration); task != nil {
				task.Status = IDLE
				c.IdleQ.Push(task)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
	return &c
}
