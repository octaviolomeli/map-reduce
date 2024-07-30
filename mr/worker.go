package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key
type ByKey []KeyValue

// for sorting by key
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker process, loop until finished
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for true {
		// Request a task from coordinator
		args := ReqTaskArgs{}
		args.WorkerId = os.Getpid()
		reply := ReqTask{}
		call("Coordinator.RequestTaskHandler", &args, &reply)

		// No more tasks, exit
		if reply.Finished {
			break
		}

		// Task is a map task
		if reply.MapTask != nil {
			ExecuteMapTask(reply.MapTask, mapf)
		}

		// Task is a reduce task
		if reply.ReduceTask != nil {
			ExecuteReduceTask(reply.ReduceTask, reducef)
		}
		// Wait some time to prevent spam requests
		time.Sleep(100 * time.Millisecond)
	}
}

// Use map function on file, sort the intermediate output, partition, and write to JSON file.
func ExecuteMapTask(task *MapTask, mapf func(string, string) []KeyValue) {
	// Get the file
	reduceCount := task.ReducerCount
	filename := task.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	// Execute map function on file content
	kva := mapf(filename, string(content))

	// Sort intermediate output by key
	sort.Sort(ByKey(kva))

	// Partition the intermediate output into reduceCount buckets
	kvap := make([][]KeyValue, reduceCount)
	for _, v := range kva {
		partitionKey := ihash(v.Key) % reduceCount
		kvap[partitionKey] = append(kvap[partitionKey], v)
	}

	// Write intermediate output to JSON file
	intermediateFiles := make([]string, reduceCount)
	for i := 0; i < reduceCount; i++ {
		interFile := fmt.Sprintf("mr-%v-%v", task.MapTaskId, i)
		intermediateFiles[i] = interFile
		osfile, _ := os.Create(interFile)

		byteValue, err := json.Marshal(kvap[i])
		if err != nil {
			fmt.Println("Marshal error: ", err)
		}
		osfile.Write(byteValue)

		osfile.Close()
	}

	// Message coordinator to track map task
	var map_task_args MapTaskArgs
	map_task_args = MapTaskArgs{
		Filename:         filename,
		IntermediateFile: intermediateFiles,
		WorkerId:         os.Getpid(),
	}
	reply := ReqTask{}
	call("Coordinator.TrackMapTaskHandler", &map_task_args, &reply)

	// Reply might have a straggler task to complete
	if reply.MapTask != nil {
		ExecuteMapTask(reply.MapTask, mapf)
	}
}

// Read intermediate file location and use reduce function on values aggregated by key
func ExecuteReduceTask(task *ReduceTask, reducef func(string, []string) string) {
	// Get the data from files
	files := task.IntermediateFile
	intermediate := []KeyValue{}

	for _, f := range files {
		data, err := ioutil.ReadFile(f)
		if err != nil {
			fmt.Println("Read error: ", err.Error())
		}
		var input []KeyValue
		err = json.Unmarshal(data, &input)
		if err != nil {
			fmt.Println("Unmarshal error: ", err.Error())
		}

		intermediate = append(intermediate, input...)
	}

	// Sort intermediate outputs by key
	sort.Sort(ByKey(intermediate))

	// Create temp file
	pattern := fmt.Sprintf("mr-out-%v", task.ReduceId)
	tempFile, err := ioutil.TempFile(".", pattern)
	if err != nil {
		fmt.Println("Error creating a temp file")
	}

	// Find start and end indices of each unique key
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		// Execute reduce function on this key and the aggregated values for it
		output := reducef(intermediate[i].Key, values)
		// Print the output
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		// Move index to next unique key
		i = j
	}

	os.Rename(tempFile.Name(), pattern)

	// Message coordinator to track reduce task
	reply := ReqTask{}
	var reduce_task_args ReduceTaskArgs
	reduce_task_args = ReduceTaskArgs{
		WorkerId: os.Getpid(),
		ReduceId: task.ReduceId}
	call("Coordinator.TrackReduceTaskHandler", &reduce_task_args, &reply)

	// Reply might have a straggler task to complete
	if reply.ReduceTask != nil {
		ExecuteReduceTask(reply.ReduceTask, reducef)
	}
}

// Send an RPC request to the coordinator, wait for the response.
// Return false if something went wrong
func call(rpcname string, args interface{}, reply interface{}) bool {
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
