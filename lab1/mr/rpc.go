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

type Action int

const (
	DoTask Action = iota
	WaitForCurrentPhaseDone
	PhaseDone
)

// Add your RPC definitions here.

type MapTaskInfo struct {
	ID       int
	Filename string // for reading file
	NReduce  int    // for spliting the task results to NReduce pieces
}

type AskMapTaskRequest struct {
	WorkerID int // just using PID
}

type AskMapTaskReply struct {
	MapTaskInfo MapTaskInfo
	Action      Action
}

type CompleteMapTaskRequest struct {
	WorkerID int // just using PID
	TaskID   int
	//
	// the results, intermediate file names
	// should order by reduce task number
	//
	Filenames []string
}

type CompleteMapTaskReply struct{}

//
// reduce
//

type ReduceTaskInfo struct {
	ID        int
	Filenames []string // for reading file
}

type AskReduceTaskRequest struct {
	WorkerID int // just using PID
}

type AskReduceTaskReply struct {
	ReduceTaskInfo ReduceTaskInfo
	Action         Action
}

type CompleteReduceTaskRequest struct {
	WorkerID int // just using PID
	TaskID   string
	Filename string // result file (only one file)
}

type CompleteReduceTaskReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
