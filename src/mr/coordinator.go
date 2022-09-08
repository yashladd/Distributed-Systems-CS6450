package mr

import (
	// "fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type JobStatus int

const (
	Pending JobStatus = iota
	InProgress
	Completed
)

type Coordinator struct {
	// Your definitions here.
	// Mapping for status and mapid of map jobs
	mapStatus   map[string]int
	fileToMapId map[string]int

	// Mapping for recude job id to reduce status
	reduceStatus map[int]int

	// Counters to track remaining map/reduce jobs
	mapJobs    int
	reduceJobs int

	//Consts based on num files and num reduce
	nReduce int
	nMaps   int
	mu      sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AllocateJob(args *RequestJob, reply *RequestJobReply) error {
	c.mu.Lock()
	if c.mapJobs > 0 {
		for file, status := range c.mapStatus {
			if status == int(Pending) {
				// fmt.Println("MapID", c.fileToMapId[file])
				reply.FileName = file
				reply.MapId = c.fileToMapId[file]
				reply.NReduce = c.nReduce
				reply.IsMapJob = true
				//mark map job in progress
				c.mapStatus[file] = int(InProgress)
				break
			}
		}
	} else if c.reduceJobs > 0 {
		for job, status := range c.reduceStatus {
			if status == int(Pending) {
				reply.IsReduceJob = true
				reply.ReduceId = job
				reply.NReduce = c.nReduce
				reply.MMaps = c.nMaps
				c.reduceStatus[job] = int(InProgress)
				break
			}
		}
	}

	c.mu.Unlock()

	return nil
}

func (c *Coordinator) JobCompleted(args *CompletedJob, reply *CompletedJobReply) error {
	c.mu.Lock()
	if args.IsMapJob {
		file := args.FileName
		if c.mapStatus[file] == int(InProgress) && c.mapJobs > 0 {
			c.mapStatus[file] = int(Completed)
			c.mapJobs -= 1
		}
	} else if args.IsReduceJob {
		reduceJob := args.ReduceId
		if c.reduceStatus[reduceJob] == int(InProgress) && c.reduceJobs > 0 {
			c.reduceStatus[reduceJob] = int(Completed)
			c.reduceJobs -= 1
		}
	}

	allJobssCompleted := c.mapJobs == 0 && c.reduceJobs == 0

	reply.Terminate = allJobssCompleted

	c.mu.Unlock()
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	c.mu.Lock()

	ret := c.mapJobs == 0 && c.reduceJobs == 0

	c.mu.Unlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.mapJobs = len(files)
	c.mapStatus = make(map[string]int)
	c.fileToMapId = make(map[string]int)
	c.reduceStatus = make(map[int]int)
	c.reduceJobs = nReduce
	c.nMaps = len(files)
	c.nReduce = nReduce

	fileNumer := 0
	for _, file := range files {
		c.mapStatus[file] = int(Pending)
		c.fileToMapId[file] = fileNumer
		fileNumer += 1
	}

	for i := 0; i < nReduce; i++ {
		c.reduceStatus[i] = int(Pending)
	}

	// fmt.Println("Map status", c.mapStatus)
	// fmt.Println("fileTomapId", c.fileToMapId)
	// fmt.Println("Pending__pending", Pending)
	// fmt.Println("Pending__int", int(Pending))

	c.server()
	return &c
}
