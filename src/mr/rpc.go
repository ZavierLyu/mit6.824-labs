package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"


// Add your RPC definitions here.
type HeartBeatArgs struct {}

type ReportArgs struct {
	TaskId int
	Type TaskType
	State bool
}

type TaskType int32
type TaskStatus int32

const (
	TaskType_Map TaskType = iota
	TaskType_Reduce
	TaskType_Wait
	TaskType_Completed
)

const (
	TaskStatus_Idle TaskStatus = iota
	TaskStatus_InProgress 
	TaskStatus_Completed
)

type HeartBeatReply struct {
	TaskId int
	Type TaskType
	Status TaskStatus
	Filename string
  	NReduce int
	NMap int
}

type ReportReply struct {}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
