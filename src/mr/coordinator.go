package mr

import (
	"crypto/sha256"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	MapTasks             []Task
	OnRunningMapTasks    []Task
	ReduceTasks          []Task
	OnRunningReduceTasks []Task
	ResultTasks          []Task
	mu                   sync.Mutex
	nReduce              int
	MapDict              map[string]int
	RedeuceDict          map[string]int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func hash1(source string, textID int, taskType string) string {
	data := fmt.Sprintf("%s-%d-%s", source, textID, taskType)

	sum := sha256.Sum256([]byte(data))

	return fmt.Sprintf("%x", sum)
}

func (c *Coordinator) GetMapTask(args *Task, reply *Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.MapTasks) == 0 {
		if len(c.OnRunningMapTasks) == 0 {
			*reply = Task{Type: "nope"}
			return nil
		} else {
			if c.OnRunningMapTasks[0].Time+10 < time.Now().Unix() {
				c.OnRunningMapTasks[0].Time = time.Now().Unix()
				c.OnRunningMapTasks[0].WorkerID = args.WorkerID
				*reply = c.OnRunningMapTasks[0]
				c.OnRunningMapTasks = c.OnRunningMapTasks[1:]
				c.OnRunningMapTasks = append(c.OnRunningMapTasks, *reply)
				c.MapDict[hash1(reply.Source, reply.SliceID, reply.Type)] = args.WorkerID
				//fmt.Printf("worker %d: call GetMapTask %s %d\n", args.WorkerID, reply.Source, reply.SliceID)
				return nil
			} else {
				*reply = Task{Type: "wait"}
				return nil
			}
		}
	}

	c.MapTasks[0].Time = time.Now().Unix()
	c.MapTasks[0].WorkerID = args.WorkerID
	*reply = c.MapTasks[0]
	c.OnRunningMapTasks = append(c.OnRunningMapTasks, *reply)
	c.MapTasks = c.MapTasks[1:]
	c.MapDict[hash1(reply.Source, reply.SliceID, reply.Type)] = args.WorkerID
	//fmt.Printf(" %s %d\n", hash1(reply.Source, reply.SliceID, reply.Type), args.WorkerID)
	return nil
}

func (c *Coordinator) GetnReduce(args *int, reply *int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	*reply = c.nReduce
	return nil
}

func (c *Coordinator) GetReduceTask(args *Task, reply *Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.ReduceTasks) == 0 {
		if len(c.OnRunningReduceTasks) == 0 {
			*reply = Task{Type: "nope"}
			return nil
		} else {
			if c.OnRunningReduceTasks[0].Time+10 < time.Now().Unix() {
				c.OnRunningReduceTasks[0].Time = time.Now().Unix()
				c.OnRunningReduceTasks[0].WorkerID = args.WorkerID
				*reply = c.OnRunningReduceTasks[0]
				c.OnRunningReduceTasks = c.OnRunningReduceTasks[1:]
				c.OnRunningReduceTasks = append(c.OnRunningReduceTasks, *reply)
				c.RedeuceDict[hash1(reply.Source, reply.SliceID, reply.Type)] = args.WorkerID
				return nil
			} else {
				*reply = Task{Type: "wait"}
				return nil
			}
		}
	}
	c.ReduceTasks[0].Time = time.Now().Unix()
	c.ReduceTasks[0].WorkerID = args.WorkerID
	*reply = c.ReduceTasks[0]
	c.OnRunningReduceTasks = append(c.OnRunningReduceTasks, *reply)
	c.ReduceTasks = c.ReduceTasks[1:]
	c.RedeuceDict[hash1(reply.Source, reply.SliceID, reply.Type)] = args.WorkerID
	return nil
}

func (c *Coordinator) SubmitMapTask(args *Task, reply *Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.WorkerID == c.MapDict[hash1(args.Source, args.SliceID, args.Type)] {
		//fmt.Printf(" %d %d\n", c.MapDict[hash1(args.Source, args.SliceID, args.Type)], args.WorkerID)
		for i, _ := range c.ReduceTasks {
			c.ReduceTasks[i].Intermediate = append(c.ReduceTasks[i].Intermediate, args.ReduceKV[i]...)
		}
		for i, task := range c.OnRunningMapTasks {
			if task.Source == args.Source && task.SliceID == args.SliceID && task.Type == args.Type {
				c.OnRunningMapTasks = append(c.OnRunningMapTasks[:i], c.OnRunningMapTasks[i+1:]...)
				break
			}
		}
	}

	return nil
}

func (c *Coordinator) SubmitReduceTask(args *Task, reply *Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.WorkerID == c.RedeuceDict[hash1(args.Source, args.SliceID, args.Type)] {
		//fmt.Printf(" done reduce\n")
		c.ResultTasks = append(c.ResultTasks, *args)
		for i, task := range c.OnRunningReduceTasks {
			if task.SliceID == args.SliceID && task.Type == args.Type {
				c.OnRunningReduceTasks = append(c.OnRunningReduceTasks[:i], c.OnRunningReduceTasks[i+1:]...)
				break
			}
		}
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := false
	//fmt.Printf("%d %d %d %d\n", len(c.MapTasks), len(c.OnRunningMapTasks), len(c.ReduceTasks), len(c.OnRunningReduceTasks))
	// Your code here.
	if len(c.MapTasks) == 0 && len(c.OnRunningMapTasks) == 0 && len(c.ReduceTasks) == 0 && len(c.OnRunningReduceTasks) == 0 {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	for i, file := range files {
		for j := 0; j < 1; j++ {
			task := Task{
				Source:   file,
				SliceID:  j,
				TextID:   i,
				SliceNum: 1,
				Type:     "map",
			}
			c.MapTasks = append(c.MapTasks, task)
		}
	}
	c.nReduce = nReduce
	c.ReduceTasks = make([]Task, nReduce)
	for i := 0; i < nReduce; i++ {
		task := Task{
			SliceID:  i,
			SliceNum: nReduce,
			Type:     "reduce",
		}
		c.ReduceTasks[i] = task
	}
	c.MapDict = make(map[string]int)
	c.RedeuceDict = make(map[string]int)

	c.server(sockname)
	return &c
}
