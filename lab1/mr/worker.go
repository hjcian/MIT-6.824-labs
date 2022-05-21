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
	printf("Start worker")
	printf("start ask map task...")

MAP_PHASE:
	for {
		task, ok := askMapTask()
		if !ok {
			// something went wrong
			return
		}

		switch task.Action {
		case WaitForCurrentPhaseDone:
			// means there is map task still processing, just wait
			time.Sleep(1 * time.Second)
			continue
		case PhaseDone:
			// go to next phase
			printf("Go to reduce phase")
			break MAP_PHASE // need this label the break the for loop, or you just break the switch case
		}
		printf("Do task: %s", task.MapTaskInfo.Filename)
		time.Sleep(200 * time.Millisecond)

		// DoTask
		// kva := readWords(reply.Filename, mapf)
		// do map
		// save intermediate files
		results := make([]string, 0)
		for i := 0; i < task.MapTaskInfo.NReduce; i++ {
			results = append(results, fmt.Sprintf("%s-%v-%v", task.MapTaskInfo.Filename, task.MapTaskInfo.ID, i))
		}
		completeMapTask(task.MapTaskInfo.ID, results)
		// send reply to master
	}

REDUCE_PHASE:
	for {
		printf("[REDUCE_PHASE] try...")

		task, ok := askReduceTask()
		if !ok {
			// something went wrong
			printf("[REDUCE_PHASE] something went wrong")
			return
		}

		switch task.Action {
		case WaitForCurrentPhaseDone:
			// means there is map task still processing, just wait
			printf("[REDUCE_PHASE] wait...")
			time.Sleep(1 * time.Second)
			continue
		case PhaseDone:
			// go to next phase
			break REDUCE_PHASE
		}

		// DoTask
		log.Println(task.ReduceTaskInfo.ID, task.ReduceTaskInfo.Filenames)
	}
	printf("End worker")
}

func askMapTask() (*AskMapTaskReply, bool) {
	reply := &AskMapTaskReply{}
	ret := call("Master.AskMapTask", &AskMapTaskRequest{os.Getpid()}, reply)
	return reply, ret
}

func completeMapTask(taskID int, results []string) {
	req := &CompleteMapTaskRequest{
		WorkerID:  os.Getpid(),
		TaskID:    taskID,
		Filenames: results,
	}
	call("Master.CompleteMapTask", req, &CompleteMapTaskReply{})
}

func askReduceTask() (*AskReduceTaskReply, bool) {
	reply := &AskReduceTaskReply{}
	ret := call("Master.AskReduceTask", &AskReduceTaskRequest{os.Getpid()}, reply)
	return reply, ret
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

	log.Println("[got master error]", err)
	return false
}

// printf adds pid in the begining and new line in the end.
func printf(foramt string, args ...interface{}) {
	prefix := fmt.Sprintf("[%v] ", os.Getpid())
	log.Printf(prefix+foramt+"\n", args...)
}
