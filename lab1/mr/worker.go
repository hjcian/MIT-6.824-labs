package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func readWords(filename string, mapf func(string, string) []KeyValue) []KeyValue {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	return kva
}

//
// main/mrworker.go calls this function.
//
func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	// require map task loop
	printf("Start worker")
	printf("start ask map task...")
	for {
		task, ok := askMapTask()
		if !ok {
			// no task possibly available, go to reduce phase
			break
		}
		if task.TaskID < 0 {
			// means there is map task, just wait
			time.Sleep(1 * time.Second)
			continue
		}
		// kva := readWords(reply.Filename, mapf)
		// do map
		// save intermediate files
		// send reply to master
	}

	printf("start ask reduce task...")
	// require reduce task loop
	for {
		task, ok := askReduceTask()
		if !ok {
			// no task possibly available, exit worker
			break
		}
		if task.TaskID < 0 {
			// means there is map task, just wait
			time.Sleep(1 * time.Second)
			continue
		}
		fmt.Println(task)
		// do reduce
		// save result file
		// send reply to master
	}
	printf("End worker")
}

func askMapTask() (*AskMapTaskReply, bool) {
	reply := &AskMapTaskReply{}
	ret := call("Master.AskMapTask", &AskMapTaskRequest{os.Getpid()}, reply)
	return reply, ret
}

func askReduceTask() (*AskReduceTaskReply, bool) {
	return nil, false
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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

// printf adds pid in the begining and new line in the end.
func printf(foramt string, args ...interface{}) {
	prefix := fmt.Sprintf("[%v] ", os.Getpid())
	log.Printf(prefix+foramt+"\n", args...)
}
