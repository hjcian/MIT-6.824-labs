package mr

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

const _defaultExpire = 10 * time.Second

type MapTask struct {
	filename            string
	mapTaskWaitingQueue chan *MapTask
	mapTaskDoneQueue    chan *MapTask
}

func (mt *MapTask) startExpireCount(taskDoneInformer context.Context) {
	go func() {
		timer := time.NewTimer(_defaultExpire)
		defer timer.Stop()

		select {
		case <-timer.C:
			// timer expired, send task back to waiting queue
			mt.mapTaskWaitingQueue <- mt
			return
		case <-taskDoneInformer.Done():
			mt.mapTaskDoneQueue <- mt
			return
		}
	}()
}

type Master struct {
	nReduce             int
	mapTaskNum          int
	mapTaskWaitingQueue chan *MapTask
	mapTaskDoneQueue    chan *MapTask
	mapTaskFinisher     map[string]context.CancelFunc
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) AskMapTask(req *AskMapTaskRequest, reply *AskMapTaskReply) error {
	if len(m.mapTaskDoneQueue) == m.mapTaskNum {
		reply.Action = PhaseDone
		fmt.Println("PhaseDone")
		return nil
	}

	mTask, ok := <-m.mapTaskWaitingQueue
	if !ok {
		reply.Action = WaitForCurrentPhaseDone
		fmt.Println("WaitForCurrentPhaseDone")
		return nil
	}
	reply.Action = DoTask
	reply.MapTaskInfo.Filename = mTask.filename
	reply.MapTaskInfo.NReduce = m.nReduce
	ctx, cancel := context.WithCancel(context.Background())
	m.mapTaskFinisher[reply.MapTaskInfo.Filename] = cancel
	mTask.startExpireCount(ctx)
	fmt.Println(reply)
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
		nReduce:             nReduce,
		mapTaskNum:          len(files),
		mapTaskWaitingQueue: make(chan *MapTask, len(files)),
		mapTaskDoneQueue:    make(chan *MapTask, len(files)),
		mapTaskFinisher:     make(map[string]context.CancelFunc),
	}
	for _, filename := range files {
		m.mapTaskWaitingQueue <- &MapTask{
			filename:            filename,
			mapTaskWaitingQueue: m.mapTaskWaitingQueue,
			mapTaskDoneQueue:    m.mapTaskDoneQueue,
		}
	}

	// create initial map tasks
	// wait:
	// 	1. worker to ask map task
	//  2. worker to commit the map task done information and each map task results (should have <nReduce> intermediate files)

	// Your code here.

	m.server()
	return &m
}
