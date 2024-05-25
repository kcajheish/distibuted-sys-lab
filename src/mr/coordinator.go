package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.
	Tasks []Task
}

const MAP = 0
const REDUCE = 1

const IDLE = 0
const IN_PROGRESS = 1
const COMPLETED = 2

type Task struct {
	TaskNumber int
	FileName   string
	JobType    string
	Status     int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) MapTask(args MapTaskArg, reply *MapTaskReply) error {
	// reply.FileName = "pg-grimm.txt"
	// reply.JobType = "map"
	// reply.TaskNumber = 1
	// reply.NumOfReduceTasks = 10
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	ret := true
	for _, task := range c.Tasks {
		if task.Status != COMPLETED {
			ret = false
			break
		}
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.server()
	return &c
}
