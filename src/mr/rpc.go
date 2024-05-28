package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskArg struct {
	TaskNumber  int
	OutputFiles []string
}

type TaskReply struct {
	Files            []string
	JobType          string
	NumOfReduceTasks int
	TaskNumber       int
}

type TaskCompleteArg struct {
	TaskNumber  int
	OutputFiles []string
}

type TaskCompleteReply struct {
	Status string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
