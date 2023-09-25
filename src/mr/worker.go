package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		if reply, ok := doHeartBeat(); ok {
			switch reply.Type {
			case TaskType_Map:
				DPrintf("[Worker]: TaskType_Map heartbeat: %v", reply)
				doMapTask(mapf, reply)
			case TaskType_Reduce:
				DPrintf("[Worker]: TaskType_Reduce heartbeat: %v", reply)
				doReduceTask(reducef, reply)
			case TaskType_Wait:
				DPrintf("[Worker]: TaskType_Wait")
				time.Sleep(1 * time.Second)
			case TaskType_Completed:
				DPrintf("[Worker]: TaskType_Completed heartbeat: %v", reply)
				return
			default:
				panic(fmt.Sprintf("Unexpected task type %v", reply.Type))
			}
		}
	}
}

func doReportTask(task HeartBeatReply, state bool) {
	args := ReportArgs{task.TaskId, task.Type, state}
	call("Coordinator.Report", &args, nil)
}

func doHeartBeat() (HeartBeatReply, bool) {
	args := HeartBeatArgs{}
	reply := HeartBeatReply{}
	ok := call("Coordinator.HeartBeat", &args, &reply)
	if !ok {
		DPrintf("[Worker]: Heartbeat failed")
		return HeartBeatReply{}, ok
	}
	return reply, ok
}

func doMapTask(mapf func(string, string) []KeyValue, task HeartBeatReply) {
	content, err := ioutil.ReadFile(task.Filename)
	if err != nil {
		DPrintf("[Worker]: cannot read %v. %v", task.Filename, err)
		doReportTask(task, false)
		return
	}
	kva := mapf(task.Filename, string(content))
	partitions := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		pid := ihash(kv.Key) % task.NReduce
		partitions[pid] = append(partitions[pid], kv)
	}
	for rid, kva := range partitions {
		fn := IntermediateFileName(task.TaskId, rid)
		f, err := os.CreateTemp("./", fn+".tmp")
		if err != nil {
			DPrintf("[Worker]: failed to create file %v. %v", fn, err)
			doReportTask(task, false)
			return
		}
		tfn := f.Name()
		defer func() {
			if err != nil {
				f.Close()
				os.Remove(tfn)
			}
		}()
		jsonEnc := json.NewEncoder(f)
		for _, kv := range kva {
			err := jsonEnc.Encode(&kv)
			if err != nil {
				DPrintf("[Worker]: failed to encode kv %v. %v", kv, err)
				return
			}
		}
		if err := f.Sync(); err != nil {
			DPrintf("[Worker]: failed to sync file %v. %v", f.Name(), err)
			doReportTask(task, false)
			return
		}
		if err := f.Close(); err != nil {
			DPrintf("[Worker]: failed to close file %v. %v", f.Name(), err)
			doReportTask(task, false)
			return
		}
		if _, err := os.Stat(fn); errors.Is(err, os.ErrNotExist) {
			os.Rename(tfn, fn)
		}
	}
	doReportTask(task, true)
}

func doReduceTask(reducef func(string, []string) string, task HeartBeatReply) {
	rid := task.TaskId
	kva := make([]KeyValue, 0)
	for mid := 0; mid < task.NMap; mid++ {
		fileName := IntermediateFileName(mid, rid)
		file, err := os.Open(fileName)
		defer file.Close()
		if err != nil {
			DPrintf("[Worker]: failed to open file %v. %v", fileName, err)
			doReportTask(task, false)
			return
		}
		jsonDec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := jsonDec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	sort.Slice(kva, func(a, b int) bool {
		return kva[a].Key < kva[b].Key
	})
	fn := fmt.Sprintf("mr-out-%v", rid)
	f, err := os.CreateTemp("./", fn+".tmp")
	if err != nil {
		DPrintf("[Worker]: failed to create file %v. %v", fn, err)
		doReportTask(task, false)
		return
	}
	tfn := f.Name() // must be f.Name() since os.CreateTemp adds random string
	defer func() {
		if err != nil {
			f.Close()
			os.Remove(tfn)
		}
	}()
	for i := 0; i < len(kva); {
		j := i
		values := []string{}
		for j < len(kva) && kva[j].Key == kva[i].Key {
			values = append(values, kva[j].Value)
			j++
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(f, "%v %v\n", kva[i].Key, output)
		i = j
	}
	if err := f.Sync(); err != nil {
		DPrintf("[Worker]: failed to sync file %v. %v", f.Name(), err)
		doReportTask(task, false)
		return
	}
	if err := f.Close(); err != nil {
		DPrintf("[Worker]: failed to close file %v. %v", f.Name(), err)
		doReportTask(task, false)
		return
	}
	if _, err := os.Stat(fn); errors.Is(err, os.ErrNotExist) {
		os.Rename(tfn, fn)
	}
	doReportTask(task, true)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
