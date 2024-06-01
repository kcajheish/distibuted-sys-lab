package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
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
	log.Printf("worker id %d start", os.Getpid())
	for {
		WorkerHelper(mapf, reducef)
	}
}
func WorkerHelper(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	resp, err := CallTask()
	if err != nil {
		log.Fatalf("can't get a task from the coordinator through RPC; err = %s\n", err)
	}

	if resp.JobType == "exit" {
		log.Printf("job complete, clsoe worker %d", os.Getegid())
		os.Exit(1)
	}

	if resp.JobType == "map" {
		f, err := os.Open(resp.Files[0])
		if err != nil {
			log.Fatalf("can't open input file; err = %s\n", err)
		}

		content, err := io.ReadAll(f)
		if err != nil {
			log.Fatalf("can't issue IO request to read input file; err = %s\n", err)
		}

		// close input file after function returns
		f.Close()
		taskNumber := resp.TaskNumber
		numOfReduceTasks := resp.NumOfReduceTasks

		log.Printf("start map worker %d\n", taskNumber)
		keyValuePairs := mapf(resp.Files[0], string(content))

		partition := map[int][]KeyValue{}
		for _, kv := range keyValuePairs {
			hashValue := ihash(kv.Key)
			partitionNumber := hashValue % numOfReduceTasks
			partition[partitionNumber] = append(partition[partitionNumber], kv)
		}

		outputFiles := make([]string, 0)
		for partitionNumber, kvPairs := range partition {
			outputFileName := fmt.Sprintf("mr-%d-%d", taskNumber, partitionNumber)
			f, err := os.CreateTemp("", "temp-map")
			if err != nil {
				log.Fatalf("can't establish IO to file %s; err = %s\n", outputFileName, err)
			}
			encoder := json.NewEncoder(f)
			for _, kv := range kvPairs {
				encoder.Encode(&kv)
			}
			f.Close()
			os.Rename(f.Name(), outputFileName)
			outputFiles = append(outputFiles, outputFileName)
		}
		CallCompleteTask(outputFiles, taskNumber, resp.JobType)
		log.Println("map worker complete task", taskNumber)
	}

	if resp.JobType == "reduce" {
		log.Println("start reduce worker, task_number=", resp.TaskNumber)
		files := resp.Files
		intermediate := []KeyValue{}
		for _, file := range files {
			f, err := os.Open(file)
			if err != nil {
				log.Fatalf("can't open file %s", file)
			}
			dec := json.NewDecoder(f)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				intermediate = append(intermediate, kv)
			}
			f.Close()
		}
		sort.Sort(ByKey(intermediate))
		outputFileName := fmt.Sprintf("mr-out-%d", resp.TaskNumber)
		f, err := os.CreateTemp("", "temp-reduce")
		if err != nil {
			log.Fatalf("can't create file for reduce task %d", resp.TaskNumber)
		}
		for i := 0; i < len(intermediate); {
			j := i
			values := []string{}
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				values = append(values, intermediate[j].Value)
				j += 1
			}
			output := reducef(intermediate[i].Key, values)
			fmt.Fprintf(f, "%v %v\n", intermediate[i].Key, output)
			i = j // next key
		}
		f.Close()
		os.Rename(f.Name(), outputFileName)
		CallCompleteTask([]string{}, resp.TaskNumber, resp.JobType)
		log.Printf("reduce task %d finishes; generate output file %s", resp.TaskNumber, outputFileName)
	}
}

type Args struct {
	TaskNumber  int
	OutputFiles []string
	JobType     string
}

type Reply struct {
	Files            []string
	JobType          string
	TaskNumber       int
	NumOfReduceTasks int
	Status           string
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

func CallCompleteTask(outputFiles []string, taskNumber int, jobType string) (Reply, error) {
	args := Args{
		OutputFiles: outputFiles,
		TaskNumber:  taskNumber,
		JobType:     jobType,
	}
	reply := Reply{}
	ok := call("Coordinator.CompleteTask", &args, &reply)
	if !ok {
		return reply, fmt.Errorf("task_number=%d failed to notify coordinator", taskNumber)
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
