package mr

import (
	"os"
	"strconv"
)

// Cook up a unique-ish UNIX-domain socket name in /var/tmp, for the coordinator.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

// RPC definitions

// Task statuses
const (
	IDLE_TASK = iota
	INPROGRESS_TASK
	COMPLETED_TASK
)

// Worker statuses
const (
	AVAILABLE_WORKER = iota
	BUSY_WORKER
)

type MapTask struct {
	Filename     string
	MapTaskId    int
	ReducerCount int
}

type MapTaskArgs struct {
	Filename         string
	IntermediateFile []string
	WorkerId         int
}

type ReduceTaskArgs struct {
	ReduceId int
	WorkerId int
}

type ReduceTask struct {
	ReduceId         int
	IntermediateFile []string
}

type ReqTaskArgs struct {
	WorkerId int
}

type ReqTask struct {
	Finished   bool
	MapTask    *MapTask
	ReduceTask *ReduceTask
}

// Used for RPC when no reply data is necessary
type EmptyReply struct {
}
