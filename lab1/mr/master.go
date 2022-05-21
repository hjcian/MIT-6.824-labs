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

type ReduceTask struct {
	id                     int
	filenames              []string
	reduceTaskWaitingQueue chan *ReduceTask
	reduceTaskDoneQueue    chan *ReduceTask
}

func (rt *ReduceTask) startExpireCount(taskDoneInformer context.Context) {
	go func() {
		timer := time.NewTimer(_defaultExpire)
		defer timer.Stop()

		select {
		case <-timer.C:
			// timer expired, send task back to waiting queue
			rt.reduceTaskWaitingQueue <- rt
			return
		case <-taskDoneInformer.Done():
			rt.reduceTaskDoneQueue <- rt
			return
		}
	}()
}

type Master struct {
	nReduce                int
	nMap                   int
	mapTaskWaitingQueue    chan *MapTask
	mapTaskDoneQueue       chan *MapTask
	reduceTaskWaitingQueue chan *ReduceTask
	reduceTaskDoneQueue    chan *ReduceTask

	mutex                      sync.RWMutex
	mapTaskProcessingBucket    map[int]context.CancelFunc
	reduceTaskProcessingBucket map[int]context.CancelFunc
	reduceTasks                [][]string
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) AskMapTask(req *AskMapTaskRequest, reply *AskMapTaskReply) error {
	if len(m.mapTaskDoneQueue) == m.nMap {
		reply.Action = PhaseDone
		log.Println("[Map] PhaseDone")
		return nil
	}

	select {
	case mt := <-m.mapTaskWaitingQueue:
		reply.MapTaskInfo.ID = mt.id
		reply.MapTaskInfo.Filename = mt.filename
		reply.MapTaskInfo.NReduce = m.nReduce
		reply.Action = DoTask
		ctx, cancel := context.WithCancel(context.Background())

		m.mutex.Lock()
		m.mapTaskProcessingBucket[mt.id] = cancel
		mt.startExpireCount(ctx)
		m.mutex.Unlock()
		return nil
	default:
		reply.Action = WaitForCurrentPhaseDone
		log.Println("[Map] WaitForCurrentPhaseDone")
		return nil
	}
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
	log.Println("[Map][task done]", req.TaskID, req.Filenames)

	delete(m.mapTaskProcessingBucket, req.TaskID)
	for reduceID, file := range req.Filenames {
		m.reduceTasks[reduceID] = append(m.reduceTasks[reduceID], file)
	}

	return nil
}

// NOTE: due to the m.mapTaskDoneQueue is queuing in another goroutine after we call done()
// we need to use mutex to protect the mapTaskDoneQueue
func (m *Master) startQueueingReduceTasks() {
	go func() {
		for {
			m.mutex.RLock()
			if len(m.mapTaskDoneQueue) != m.nMap {
				m.mutex.RUnlock()
				// wait for map tasks done
				time.Sleep(time.Second)
				continue
			}

			log.Println("start queuing reduce tasks")
			for reduceID, tasks := range m.reduceTasks {
				m.reduceTaskWaitingQueue <- &ReduceTask{
					id:                     reduceID,
					filenames:              tasks,
					reduceTaskWaitingQueue: m.reduceTaskWaitingQueue,
					reduceTaskDoneQueue:    m.reduceTaskDoneQueue,
				}
			}
			m.mutex.RUnlock()
			log.Println("finish queuing reduce tasks")
			break // exit the loop
		}
	}()
}

func (m *Master) AskReduceTask(req *AskReduceTaskRequest, reply *AskReduceTaskReply) error {
	log.Println("[Reduce] call AskReduceTask")

	if len(m.mapTaskDoneQueue) == m.nReduce {
		reply.Action = PhaseDone
		log.Println("[Reduce] PhaseDone")
		return nil
	}

	select {
	case rt := <-m.reduceTaskWaitingQueue:
		reply.ReduceTaskInfo.ID = rt.id
		reply.ReduceTaskInfo.Filenames = rt.filenames
		reply.Action = DoTask
		ctx, cancel := context.WithCancel(context.Background())

		m.mutex.Lock()
		m.mapTaskProcessingBucket[rt.id] = cancel
		rt.startExpireCount(ctx)
		m.mutex.Unlock()
		return nil

	default:
		reply.Action = WaitForCurrentPhaseDone
		log.Println("[Reduce] WaitForCurrentPhaseDone")
		return nil
	}
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
	nReduce = 3

	taskNum := len(files)
	m := Master{
		nReduce:                 nReduce,
		nMap:                    taskNum,
		mapTaskWaitingQueue:     make(chan *MapTask, taskNum),
		mapTaskDoneQueue:        make(chan *MapTask, taskNum),
		mapTaskProcessingBucket: make(map[int]context.CancelFunc),

		reduceTaskWaitingQueue:     make(chan *ReduceTask, nReduce),
		reduceTaskDoneQueue:        make(chan *ReduceTask, nReduce),
		reduceTaskProcessingBucket: make(map[int]context.CancelFunc),
		reduceTasks:                make([][]string, nReduce),
	}

	for idx, filename := range files {
		m.mapTaskWaitingQueue <- &MapTask{
			id:                  idx,
			filename:            filename,
			mapTaskWaitingQueue: m.mapTaskWaitingQueue,
			mapTaskDoneQueue:    m.mapTaskDoneQueue,
		}
	}

	for i := 0; i < nReduce; i++ {
		m.reduceTasks[i] = make([]string, 0, taskNum)
	}

	m.startQueueingReduceTasks()
	// create initial map tasks
	// wait:
	// 	1. worker to ask map task
	//  2. worker to commit the map task done information and each map task results (should have <nReduce> intermediate files)

	// Your code here.

	m.server()
	return &m
}
