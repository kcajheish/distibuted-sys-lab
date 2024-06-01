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
	Tasks            []Task
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
	log.Printf("master %d assign %s task %d to worker", os.Getegid(), task.TaskNumber, jType)
	return nil
}

func (c *Coordinator) CompleteTask(args TaskCompleteArg, reply *TaskCompleteReply) error {
	log.Printf("master %d receives complete %s task %d", os.Getegid(), args.JobType, args.TaskNumber)
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
		c.Counter.Count += 1
		if c.Counter.Count == c.NumOfMapTasks {
			log.Printf("master %d writes out all partitions from memory to files", os.Getegid())
			for p := 0; p < c.NumOfReduceTasks; p++ {
				files := c.Partitions.Read(p)
				reduceTask := Task{
					TaskNumber: p + c.NumOfMapTasks,
					Files:      files,
					JobType:    REDUCE,
					Status:     IDLE,
					UnixTime:   time.Now().UnixMicro(),
				}
				c.IdleQ.Push(&reduceTask)
				c.Tasks = append(c.Tasks, reduceTask)
			}
			log.Println("master %d wake up all sleeping reduce worker", os.Getegid())
			c.Counter.Broadcast()
		}

		c.Counter.Unlock()
		completedTask := c.Tasks[args.TaskNumber]

		c.ProcessPQ.Done(&completedTask)
		log.Printf("master %d marks %d task as compelted", os.Getegid(), args.TaskNumber)
	}
	reply.Status = "success"
	log.Printf("%s task %d finishes", args.JobType, args.TaskNumber)

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
	c.Tasks = make([]Task, 0, c.NumOfMapTasks+c.NumOfReduceTasks)
	for i, file := range files {
		task := Task{
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
		c.IdleQ.Push(&task)
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
	log.Printf("start master id=%d", os.Getegid())
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
		maxDuration := int64(math.Pow10(6) * 10) // total us in a second
		for {
			now := time.Now().UnixMicro()
			if task := c.ProcessPQ.ExpireAndPop(now, maxDuration); task != nil {
				c.IdleQ.Push(task)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
	return &c
}
