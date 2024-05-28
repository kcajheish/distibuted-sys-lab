package mr

import (
	"container/heap"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	Tasks            []Task
	IdleQ            PriorityQueue
	ProcessPQ        PriorityQueue
	NumOfReduceTasks int
	NumOfMapTasks    int
}

const MAP = 0
const REDUCE = 1

const IDLE = 0
const IN_PROGRESS = 1
const COMPLETED = 2

type Task struct {
	TaskNumber int
	FileName   string
	JobType    int
	Status     int
	UnixTime   int64
	index      int // position at the priority queue
}

func (c *Coordinator) GetTask(args TaskArg, reply *TaskReply) error {
	task := heap.Pop(&c.IdleQ).(*Task)
	task.UnixTime = time.Now().UnixMicro()
	c.ProcessPQ.Push(task)
	reply.FileName = task.FileName
	if task.JobType == MAP {
		reply.JobType = "map"
	} else {
		reply.JobType = "reduce"
	}
	reply.TaskNumber = task.TaskNumber
	reply.NumOfReduceTasks = c.NumOfReduceTasks
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
			FileName:   file,
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
