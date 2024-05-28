package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	resp, err := CallTask()
	if err != nil {
		log.Fatalf("can't get a task from the coordinator through RPC; err = %s\n", err)
	}

	f, err := os.Open(resp.FileName)
	if err != nil {
		log.Fatalf("can't open input file; err = %s\n", err)
	}

	content, err := io.ReadAll(f)
	if err != nil {
		log.Fatalf("can't issue IO request to read input file; err = %s\n", err)
	}

	// close input file after function returns
	f.Close()

	if resp.JobType == "map" {
		taskNumber := resp.TaskNumber
		numOfReduceTasks := resp.NumOfReduceTasks

		log.Printf("start map worker %d\n", taskNumber)
		keyValuePairs := mapf(resp.FileName, string(content))

		log.Print("start partitioning key\n")
		partition := map[int][]KeyValue{}
		for _, kv := range keyValuePairs {
			hashValue := ihash(kv.Key)
			partitionNumber := hashValue % numOfReduceTasks
			partition[partitionNumber] = append(partition[partitionNumber], kv)
		}

		log.Println("start writing intermediate key/value into output partition files")
		outputFiles := make([]string, 0)
		for partitionNumber, kvPairs := range partition {
			outputFileName := fmt.Sprintf("mr-out-%d-%d", taskNumber, partitionNumber)
			f, err := os.OpenFile(outputFileName, os.O_RDWR|os.O_CREATE, 0644)
			if err != nil {
				log.Fatalf("can't establish IO to file %s; err = %s\n", outputFileName, err)
			}
			encoder := json.NewEncoder(f)
			for _, kv := range kvPairs {
				encoder.Encode(&kv)
			}
			f.Close()
			outputFiles = append(outputFiles, outputFileName)
		}
		CallCompleteTask(outputFiles, taskNumber)
		log.Println("worker complete task", taskNumber)
	}
}

type Args struct {
	TaskNumber  int
	OutputFiles []string
}

type Reply struct {
	FileName         string
	JobType          string
	TaskNumber       int
	NumOfReduceTasks int
}

func CallTask() (Reply, error) {
	args := Args{}
	reply := Reply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		return reply, fmt.Errorf("failed")
	} else {
		return reply, nil
	}

}

func CallCompleteTask(outputFiles []string, taskNumber int) (Reply, error) {
	args := Args{
		OutputFiles: outputFiles,
		TaskNumber:  taskNumber,
	}
	reply := Reply{}
	ok := call("Coordinator.CompleteTask", &args, &reply)
	if !ok {
		return reply, fmt.Errorf("task_number=%d, output_file=%s failed to notify coordinator", taskNumber)
	} else {
		return reply, nil
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
