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
const EXIT = 2

const IDLE = 0
const IN_PROGRESS = 1
const COMPLETED = 2

const NO_ASSIGNED_WORKER = -1

var typeMap = map[int]string{
	MAP:    "map",
	REDUCE: "reduce",
	EXIT:   "exit",
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
	Assigned   int
}

func (c *Coordinator) pollTask() *Task {
	c.Counter.Lock()
	var task *Task
	total := c.NumOfMapTasks + c.NumOfReduceTasks
	for {
		task = c.IdleQ.Pop()
		if task == nil {
			if c.Counter.Count > c.NumOfMapTasks && c.Counter.Count < total {
				c.IdleQ.Push(&Task{
					JobType: EXIT,
				})
				c.Counter.Wait()
			}
			if c.Counter.Count == total {
				task = &Task{
					JobType: EXIT,
				}
				break
			}
		} else {
			break
		}
	}
	c.Counter.Unlock()
	return task
}

func (c *Coordinator) GetTask(args TaskArg, reply *TaskReply) error {
	task := c.pollTask()
	if task.JobType == EXIT {
		log.Printf("master %d ask worker to exit", os.Getpid())
		reply.JobType = typeMap[task.JobType]
		return nil
	}
	task.UnixTime = time.Now().UnixMicro()
	task.Status = IN_PROGRESS
	task.Assigned = args.WorkerID
	c.ProcessPQ.Push(task)
	reply.Files = task.Files
	reply.JobType = typeMap[task.JobType]
	reply.TaskNumber = task.TaskNumber
	reply.NumOfReduceTasks = c.NumOfReduceTasks
	log.Printf("master %d assign %s task %d to worker", os.Getpid(), typeMap[task.JobType], task.TaskNumber)
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
			Assigned:   NO_ASSIGNED_WORKER,
		}
		c.IdleQ.Push(task)
		c.Tasks = append(c.Tasks, task)
	}
}

func (c *Coordinator) CompleteTask(args TaskCompleteArg, reply *TaskCompleteReply) error {
	task := c.Tasks[args.TaskNumber]
	if task.Assigned != args.WorkerID {
		log.Printf("master %d receives duplicated completed task %s from worker %d; task is now run by worker %d; ignore request", os.Getpid(), task.JobType, args.WorkerID, task.Assigned)
		return nil
	}
	log.Printf("master %d receives completed %s task %d", os.Getpid(), args.JobType, args.TaskNumber)
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
			if c.Counter.Count == c.NumOfMapTasks+c.NumOfReduceTasks {
				c.Counter.Broadcast()
			}
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
			Assigned:   NO_ASSIGNED_WORKER,
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
				task.Assigned = NO_ASSIGNED_WORKER
				c.IdleQ.Push(task)
				c.Counter.Lock()
				c.Counter.Signal()
				c.Counter.Unlock()
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
	return &c
}
