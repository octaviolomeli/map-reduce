package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	intermediateFiles map[int][]string      // Partition # to files
	workerToStatus    map[int]int           // worker ID to status
	mapToStatus       map[string]TaskStatus // filename to status
	mapWasReExec      map[string]bool       // If this map task was re-executed
	reduceWasReExec   map[int]bool          // If this reduce task was re-executed
	reduceToStatus    map[int]TaskStatus    // Partition # to status
	mapTaskNumber     int                   // For making map task ids
	nReducer          int                   // # of partitions to make
	mutexLock         sync.Mutex            // For locking shared data
}

type TaskStatus struct {
	StartTime int64
	Status    int
}

// Start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Create a Coordinator
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Initialize values
	c.mapTaskNumber = 0
	c.nReducer = nReduce
	c.mapToStatus = make(map[string]TaskStatus)
	for _, v := range files {
		c.mapToStatus[v] = TaskStatus{StartTime: -1, Status: IDLE_TASK}
	}

	c.reduceToStatus = make(map[int]TaskStatus)
	for i := 0; i < nReduce; i++ {
		c.reduceToStatus[i] = TaskStatus{StartTime: -1, Status: IDLE_TASK}
	}

	c.workerToStatus = make(map[int]int)
	c.mapWasReExec = make(map[string]bool)
	c.reduceWasReExec = make(map[int]bool)
	c.intermediateFiles = make(map[int][]string)
	c.StartTimer()
	c.server()
	return &c
}

// Check if all tasks are completed
func (c *Coordinator) Done() bool {
	c.mutexLock.Lock()
	defer c.mutexLock.Unlock()
	for _, status := range c.reduceToStatus {
		if status.Status != COMPLETED_TASK {
			return false
		}
	}

	return true
}

// Find task to hand out or none if all completed
// Executed when worker sends message to coordinator
func (c *Coordinator) RequestTaskHandler(args *ReqTaskArgs, reply *ReqTask) error {
	c.mutexLock.Lock()
	defer c.mutexLock.Unlock()

	// New worker
	if c.workerToStatus[args.WorkerId] == 0 {
		c.workerToStatus[args.WorkerId] = AVAILABLE_WORKER
	}

	mapTask := c.createMapTask()
	// Check if Map tasks still exists
	if mapTask != nil {
		c.workerToStatus[args.WorkerId] = BUSY_WORKER
		reply.Finished = false
		reply.MapTask = mapTask
		return nil
	}

	reduceTask := c.createReduceTask()
	// Check if Reduce tasks still exists
	if reduceTask != nil {
		c.workerToStatus[args.WorkerId] = BUSY_WORKER
		reply.Finished = false
		reply.ReduceTask = reduceTask
		return nil
	}

	// Check if all map tasks are completed
	for _, status := range c.mapToStatus {
		if status.Status != COMPLETED_TASK {
			reply.Finished = false
			reply.MapTask = mapTask
			return nil
		}
	}

	reply.Finished = c.Done()
	return nil
}

// Track status for a map task, executed when a worker completes a map task
func (c *Coordinator) TrackMapTaskHandler(args *MapTaskArgs, reply *ReqTask) error {
	c.mutexLock.Lock()
	defer c.mutexLock.Unlock()
	// Set task as completed
	c.mapToStatus[args.Filename] = TaskStatus{StartTime: -1, Status: COMPLETED_TASK}
	// Set worker as available for another task
	c.workerToStatus[args.WorkerId] = AVAILABLE_WORKER
	// Store location of output
	for r := 0; r < c.nReducer; r++ {
		c.intermediateFiles[r] = append(c.intermediateFiles[r], args.IntermediateFile[r])
	}

	// See if there's a straggler task and if conditions are right, re-execute

	// Find possible remaining tasks to re-execute for stragglers if no other task is idle
	var possibleTask string
	now := time.Now().Unix()
	for mapTask, status := range c.mapToStatus {
		if status.Status == IDLE_TASK {
			return nil
		} else if c.mapWasReExec[mapTask] == false && status.Status == INPROGRESS_TASK && now >= (status.StartTime+5) {
			possibleTask = mapTask
		}
	}

	// If no idle tasks, get a map task to re-execute for stragglers
	if possibleTask != "" {
		c.workerToStatus[args.WorkerId] = BUSY_WORKER
		reply.Finished = false
		reply.MapTask = &MapTask{
			Filename:     possibleTask,
			MapTaskId:    c.mapTaskNumber,
			ReducerCount: c.nReducer,
		}
		c.mapTaskNumber += 1
		c.mapWasReExec[possibleTask] = true
	}

	return nil
}

// Track status for a reduce task, executed when a worker completes a reduce task
func (c *Coordinator) TrackReduceTaskHandler(args *ReduceTaskArgs, reply *ReqTask) error {
	c.mutexLock.Lock()
	defer c.mutexLock.Unlock()
	// Set task as completed
	c.reduceToStatus[args.ReduceId] = TaskStatus{StartTime: -1, Status: COMPLETED_TASK}
	// Set worker as available for another task
	c.workerToStatus[args.WorkerId] = AVAILABLE_WORKER

	// See if there's a straggler task and if conditions are right, re-execute

	// Find possible remaining tasks to re-execute for stragglers if no other task is idle
	possibleTask := -1
	now := time.Now().Unix()
	for reduceTask, status := range c.reduceToStatus {
		if status.Status == IDLE_TASK {
			return nil
		} else if c.reduceWasReExec[reduceTask] == false && status.Status == INPROGRESS_TASK && now >= (status.StartTime+5) {
			possibleTask = reduceTask
		}
	}

	// If no idle tasks, get a reduce task to re-execute for stragglers
	if possibleTask != -1 {
		c.workerToStatus[args.WorkerId] = BUSY_WORKER
		reply.Finished = false
		reply.ReduceTask = &ReduceTask{
			ReduceId:         possibleTask,
			IntermediateFile: c.intermediateFiles[possibleTask],
		}
		c.reduceWasReExec[possibleTask] = true
	}

	return nil
}

// Create a task and populate with information
func (c *Coordinator) createMapTask() *MapTask {
	var task *MapTask = nil
	for file, statusObject := range c.mapToStatus {
		// Find first idle task
		if statusObject.Status == IDLE_TASK {
			// Populate reply with information for worker
			task = &MapTask{}
			task.MapTaskId = c.mapTaskNumber
			task.Filename = file
			c.mapTaskNumber++
			task.ReducerCount = c.nReducer
			c.mapToStatus[file] = TaskStatus{StartTime: time.Now().Unix(), Status: INPROGRESS_TASK}
			break
		}
	}

	return task
}

// Select a task to hand out and time it
func (c *Coordinator) createReduceTask() *ReduceTask {
	var task *ReduceTask = nil
	reducer := -1
	for i, statusObject := range c.reduceToStatus {
		// Find first idle task
		if statusObject.Status == IDLE_TASK {
			reducer = i
			break
		}
	}

	if reducer < 0 {
		return nil
	}

	// Populate reply with information for worker
	task = &ReduceTask{}
	task.IntermediateFile = c.intermediateFiles[reducer]
	task.ReduceId = reducer
	c.reduceToStatus[reducer] = TaskStatus{StartTime: time.Now().Unix(), Status: INPROGRESS_TASK}

	return task
}

// Start timing for a task, used to determine if a worker is slow
func (c *Coordinator) StartTimer() {
	ticker := time.NewTicker(10 * time.Second)

	go func() {
		for {
			select {
			case <-ticker.C:
				if c.Done() {
					return
				}
				c.RestartSlowWorkerTasks()
			}
		}
	}()
}

// Go through tasks and for running tasks, check if more than 10 seconds have elapsed.
// Restart the task if true
func (c *Coordinator) RestartSlowWorkerTasks() {
	c.mutexLock.Lock()
	defer c.mutexLock.Unlock()
	for file, statusObject := range c.mapToStatus {
		// If task is in progress, check time
		if statusObject.Status == INPROGRESS_TASK {
			now := time.Now().Unix()
			// Set task as idle for another worker to pick up
			if statusObject.StartTime > 0 && now > (statusObject.StartTime+10) {
				c.mapToStatus[file] = TaskStatus{StartTime: -1, Status: IDLE_TASK}
				continue
			}
		}
	}

	for file, statusObject := range c.reduceToStatus {
		// If task is in progress, check time
		if statusObject.Status == INPROGRESS_TASK {
			now := time.Now().Unix()
			// Set task as idle for another worker to pick up
			if statusObject.StartTime > 0 && now > (statusObject.StartTime+10) {
				c.reduceToStatus[file] = TaskStatus{StartTime: -1, Status: IDLE_TASK}
				continue
			}
		}
	}
}
