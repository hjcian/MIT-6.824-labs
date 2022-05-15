package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Master struct {
	// Your definitions here.
	mu          sync.Mutex
	mapTasks    []string
	reduceTasks []string
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) GetMapTask(_ *MapTaskRequest, reply *MapTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.mapTasks) == 0 {
		return errors.New("no job")
	}

	reply.Filename = m.mapTasks[0]
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	fmt.Println("Start master")
	fmt.Println(files)
	m := Master{
		mapTasks:    files,
		reduceTasks: make([]string, 0),
	}

	// Your code here.

	m.server()
	return &m
}
