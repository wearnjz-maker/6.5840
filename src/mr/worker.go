package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

func getPart(filename string, taskID int, totalTasks int) string {
	data, _ := os.ReadFile(filename)

	lines := strings.Split(string(data), "\n")

	// 每份多少行
	partSize := len(lines) / totalTasks

	start := taskID * partSize

	end := start + partSize

	// 最后一份包含剩余行
	if taskID == totalTasks-1 {
		end = len(lines)
	}

	return strings.Join(lines[start:end], "\n")
}

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname
	args := 0
	reply := 0
	err := call("Coordinator.GetnReduce", &args, &reply)
	if err != true {
		log.Fatalf("call Coordinator.GetnReduce failed")
	}
	nReduce := reply
	//fmt.Printf("worker %d: get nReduce %d\n", os.Getpid(), nReduce)
Loop:
	for {
		task, ok := GetMapTask()
		if !ok {
			break
		}
		switch task.Type {
		case "nope":
			break Loop
		case "map":
			// 执行map任务
			intermediate := mapf(task.Source, getPart(task.Source, task.SliceID, task.SliceNum))
			// 将中间结果写入文件
			//sort.Sort(ByKey(intermediate))
			reduceTasks := make([][]KeyValue, nReduce)
			for _, kv := range intermediate {
				/*if kv.Key == "a" {
					fmt.Printf("%d\n", ihash(kv.Key)%nReduce)
				}*/
				reduceTaskNum := ihash(kv.Key) % nReduce
				reduceTasks[reduceTaskNum] = append(reduceTasks[reduceTaskNum], kv)
			}
			SubmitMapTask(task, reduceTasks)
			break
		case "wait":
			time.Sleep(1 * time.Second)
			break
		}
	}
Loop1:
	for {
		task, ok := GetReduceTask()

		if !ok {
			break
		}
		switch task.Type {
		case "reduce":
			intermediate := task.Intermediate
			task.Intermediate = nil
			// 将中间结果写入文件
			task.Type = "reduce"
			filename := fmt.Sprintf("mr-out-%d", task.SliceID)
			ofile, _ := os.Create(filename)
			sort.Sort(ByKey(intermediate))
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			task.Type = "reduce"
			task.OFile = filename
			//pwd, _ := os.Getwd()
			//fmt.Println("output dir:", pwd)
			SubmitReduceTask(task)
		case "wait":
			time.Sleep(10 * time.Second)
		case "nope":
			break Loop1
		}
	}
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func GetMapTask() (Task, bool) {
	args := Task{}
	reply := Task{}
	args.WorkerID = os.Getpid()

	err := call("Coordinator.GetMapTask", &args, &reply)

	if err != true {
		return reply, false
	}

	return reply, true
}
func GetReduceTask() (Task, bool) {
	args := Task{}
	reply := Task{}
	args.WorkerID = os.Getpid()
	ok := call("Coordinator.GetReduceTask", args, &reply)
	if ok != true {
		return reply, false
	}
	return reply, true
}
func SubmitMapTask(task Task, reduceTasks [][]KeyValue) {
	task.ReduceKV = make([][]KeyValue, len(reduceTasks))
	copy(task.ReduceKV, reduceTasks)
	call("Coordinator.SubmitMapTask", task, &task)
}
func SubmitReduceTask(task Task) {
	args := task
	reply := Task{}

	call("Coordinator.SubmitReduceTask", args, &reply)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
