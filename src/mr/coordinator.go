package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

const (
	PROCESS_TIME time.Duration = 10 * time.Second 
	TERMINATE_TIME time.Duration = 10 * time.Second
)

type Coordinator struct {
	splits 	[]string
	nMap 	int
	nReduce int
	tasks 	[]Task
	phase 	JobPhase
	lastCallAfterDoneTime time.Time

	heartbeatCh chan heartBeatCall
	reportCh 	chan reportCall
	
	// calls from monitor
	tranPhaseCh	chan tranCall
	delayCh		chan delayCall
	terminateCh chan struct{}
}

type Task struct {
	filename string
	taskId int
	taskType TaskType
	taskStatus TaskStatus
	startTime time.Time
}

type JobPhase int32
type WarningType int32

const (
	JobPhase_Map JobPhase = iota
	JobPhase_Reduce
	JobPhase_Done
)

const (
	WarningType_Timeout = iota
	WarningTYpe_Unknown
)

type heartBeatCall struct {
	args  *HeartBeatArgs
	reply *HeartBeatReply
	ok chan struct{}
}

type reportCall struct {
	args  *ReportArgs
	reply *ReportReply
	ok chan struct{}
}

type delayCall struct {
	taskId 	int
	ok 		chan struct{}
}

type tranCall struct {
	map2reduce 		bool
	reduce2done	bool
	ok				chan struct{}
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) HeartBeat(args *HeartBeatArgs, reply *HeartBeatReply) error {
	call := heartBeatCall{
		args: args,
		reply: reply,
		ok: make(chan struct{}),
	}
	c.heartbeatCh <- call
	<-call.ok
	return nil
}

func (c *Coordinator) Report(args *ReportArgs, reply *ReportReply) error {
	call := reportCall{
		args: args,
		reply: reply,
		ok: make(chan struct{}),
	}
	c.reportCh <- call
	<-call.ok
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// initialize map tasks 
func (c *Coordinator) init() {
	tasks := make([]Task, 0, len(c.splits))
	for i, file := range c.splits {
		tasks = append(tasks, Task{
			filename: file,
			taskId: i,
			taskType: TaskType_Map,
			taskStatus: TaskStatus_Idle,
			startTime: time.Time{},
		})
	}
	c.tasks = tasks
}

func (c *Coordinator) monitor() {
	// todo
	// 1. poll all the tasks and check time and send necessary call to the channel
	// 2. change the job phase accordinigly
	for {
		minStartTime := time.Now().Add(-PROCESS_TIME)
		doneCnt := 0
		for _, t := range c.tasks {
			if (t.taskStatus == TaskStatus_InProgress) {
				if (t.startTime.Before(minStartTime)) {
					call := delayCall {
						taskId: t.taskId,
						ok: make(chan struct{}),
					}
					c.delayCh <- call
					<-call.ok
				}
			} else if t.taskStatus == TaskStatus_Completed {
				doneCnt++
			}
		}
		if doneCnt == len(c.tasks) {
			call := tranCall {
				map2reduce: c.phase == JobPhase_Map,
				reduce2done: c.phase == JobPhase_Reduce,
				ok: make(chan struct{}),
			}
			DPrintf("[Coordinator monitor] donecnt:%v call:%v", doneCnt, call)
			c.tranPhaseCh <- call
			<-call.ok
		}
		if c.phase == JobPhase_Done {
			DPrintf("[Coordinator] terminate monitor()")
			return
		}
	}
}

func (c *Coordinator) run() {
	for {
		select {
		case call := <-c.heartbeatCh:
			c.handleHeartBeat(call)
			call.ok <- struct{}{}
		case call := <-c.reportCh:
			c.handleReport(call)
			call.ok <- struct{}{}
		case call := <-c.delayCh:
			c.handleDelay(call)
			call.ok <- struct{}{}
		case call := <-c.tranPhaseCh:
			c.handleTranPhase(call)
			call.ok <- struct{}{}
		case <-c.terminateCh:
			DPrintf("[Coordinator] terminate run()")
			return
		}
	}
}

func (c *Coordinator) handleTranPhase(call tranCall) {
	if call.map2reduce {
		c.tasks = nil
		for rid := 0; rid < c.nReduce; rid++ {
			c.tasks = append(c.tasks, Task{
				taskId: rid,
				taskType: TaskType_Reduce,
				taskStatus: TaskStatus_Idle,
			})
		}
		DPrintf("[Coordinator tranphase]: map2reduce")
		c.phase = JobPhase_Reduce
		} else if call.reduce2done {
		DPrintf("[Coordinator tranphase]: reduce2done")
		c.tasks = nil
		c.phase = JobPhase_Done
	}
}

func (c *Coordinator) handleDelay(call delayCall) {
	taskId := call.taskId
	c.tasks[taskId].taskStatus = TaskStatus_Idle
}

func (c *Coordinator) handleHeartBeat(call heartBeatCall) {
	reply := call.reply
	switch c.phase {
	case JobPhase_Map, JobPhase_Reduce:
		for _, t := range c.tasks {
			if (t.taskStatus == TaskStatus_Idle) {
				reply.TaskId = t.taskId
				reply.Type = t.taskType
				reply.Status = TaskStatus_InProgress
				reply.Filename = t.filename
				reply.NReduce = c.nReduce
				reply.NMap = c.nMap
				t.taskStatus = TaskStatus_InProgress
				t.startTime = time.Now()
				return
			}
		}
		reply.Type = TaskType_Wait;
	case JobPhase_Done:
		c.lastCallAfterDoneTime = time.Now()
		reply.Type = TaskType_Completed
	default:
		reply.Type = TaskType_Wait
	}
}

func (c *Coordinator) handleReport(call reportCall) {
	taskId := call.args.TaskId
	state := call.args.State
	DPrintf("[Coordinator]: handle report id:%v state:%v phase:%v", taskId, state, c.phase)
	if state == true {
		c.tasks[taskId].taskStatus = TaskStatus_Completed
	} else {
		c.tasks[taskId].taskStatus = TaskStatus_Idle
	}
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	done := (c.phase == JobPhase_Done && c.lastCallAfterDoneTime.Add(TERMINATE_TIME).Before(time.Now()))
	if done {
		DPrintf("[Coordinator] ending...")
		c.terminateCh <- struct{}{}
	}
	return done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	c := Coordinator{
		splits: files,
		nMap: len(files),
		nReduce: nReduce,
		tasks: nil,
		phase: JobPhase_Map,
		lastCallAfterDoneTime: time.Time{},

		heartbeatCh: make(chan heartBeatCall),
		reportCh: make(chan reportCall),
		tranPhaseCh: make(chan tranCall),
		delayCh: make(chan delayCall),
		terminateCh: make(chan struct{}),
	}
	c.init()
	c.server()
	go c.run()
	go c.monitor()
	return &c
}
