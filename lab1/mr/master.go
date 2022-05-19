package mr

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const _defaultExpire = 10 * time.Second

type MapTask struct {
	id                  int
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

	mutex                   sync.Mutex
	mapTaskProcessingBucket map[int]context.CancelFunc
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) AskMapTask(req *AskMapTaskRequest, reply *AskMapTaskReply) error {
	if len(m.mapTaskDoneQueue) == m.mapTaskNum {
		reply.Action = PhaseDone
		log.Println("PhaseDone")
		return nil
	}

	mTask, ok := <-m.mapTaskWaitingQueue
	if !ok {
		reply.Action = WaitForCurrentPhaseDone
		log.Println("WaitForCurrentPhaseDone")
		return nil
	}
	reply.Action = DoTask
	reply.MapTaskInfo.ID = mTask.id
	reply.MapTaskInfo.Filename = mTask.filename
	reply.MapTaskInfo.NReduce = m.nReduce
	ctx, cancel := context.WithCancel(context.Background())

	m.mutex.Lock()
	m.mapTaskProcessingBucket[mTask.id] = cancel
	mTask.startExpireCount(ctx)
	m.mutex.Unlock()
	return nil
}

func (m *Master) CompleteMapTask(req *CompleteMapTaskRequest, reply *CompleteMapTaskReply) error {
	if len(req.Filenames) != m.nReduce {
		return errors.New("are you kidding me? why your results not match nReduce?")
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	done, ok := m.mapTaskProcessingBucket[req.TaskID]
	if !ok {
		return errors.New("task not found")
	}
	done() // call cancel, means job done
	log.Println("[job done]", req.TaskID, len(m.mapTaskDoneQueue), req.Filenames)

	delete(m.mapTaskProcessingBucket, req.TaskID)
	// TODO: store reduce tasks information
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

	taskNum := len(files)
	m := Master{
		nReduce:                 nReduce,
		mapTaskNum:              taskNum,
		mapTaskWaitingQueue:     make(chan *MapTask, taskNum),
		mapTaskDoneQueue:        make(chan *MapTask, taskNum),
		mapTaskProcessingBucket: make(map[int]context.CancelFunc),
	}

	for idx, filename := range files {
		m.mapTaskWaitingQueue <- &MapTask{
			id:                  idx,
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
