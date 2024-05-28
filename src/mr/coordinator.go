package mr

import (
	"container/heap"
	"log"
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
	IdleQ            PriorityQueue
	ProcessPQ        PriorityQueue
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

func (c *Coordinator) GetTask(args TaskArg, reply *TaskReply) error {
	c.Counter.Lock()
	for c.IdleQ.Len() == 0 && c.Counter.Count < c.NumOfMapTasks {
		c.Counter.Wait()
	}
	if c.IdleQ.Len() == 0 && c.Counter.Count == c.NumOfMapTasks {
		number := c.NumOfMapTasks
		for p := 0; p < c.NumOfMapTasks; p++ {
			files := c.Partitions.Read(p)
			reduceTask := &Task{
				TaskNumber: number,
				Files:      files,
				JobType:    REDUCE,
				Status:     IDLE,
				UnixTime:   time.Now().UnixMicro(),
				index:      p,
			}
			heap.Push(&c.IdleQ, reduceTask)
		}
	}
	c.Counter.Unlock()

	task := heap.Pop(&c.IdleQ).(*Task)
	task.UnixTime = time.Now().UnixMicro()
	c.ProcessPQ.Push(task)
	reply.Files = task.Files
	if task.JobType == MAP {
		reply.JobType = "map"
	} else {
		reply.JobType = "reduce"
	}
	reply.TaskNumber = task.TaskNumber
	reply.NumOfReduceTasks = c.NumOfReduceTasks
	return nil
}

func (c *Coordinator) CompleteTask(args TaskCompleteArg, reply *TaskCompleteReply) error {
	for _, file := range args.OutputFiles {
		words := strings.Split(file, "-")
		partitionNumber, err := strconv.Atoi(words[3]) // mr-out-<map number>-<partition number>
		if err != nil {
			log.Fatal("can't convert %s to partition number", partitionNumber)
		}
		c.Partitions.Append(partitionNumber, file)
	}
	c.Counter.Count += 1
	reply.Status = "success"
	log.Println(c.Partitions.partitions)
	log.Printf("map task %d finishes", args.TaskNumber)
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
	a := c.IdleQ.Len()
	b := c.ProcessPQ.Len()
	return a+b == 0
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
	c.IdleQ = PriorityQueue{}
	c.IdleQ.tasks = make([]*Task, 0, c.NumOfMapTasks+c.NumOfReduceTasks)
	for _, task := range c.Tasks {
		c.IdleQ.Push(&task)
	}
}

func (c *Coordinator) initProcessQueue() {
	c.ProcessPQ = PriorityQueue{}
	c.ProcessPQ.tasks = make([]*Task, 0, c.NumOfMapTasks+c.NumOfReduceTasks)
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
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
	return &c
}
