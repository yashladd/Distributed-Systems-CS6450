package mr

import (
	// "fmt"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
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
	// fmt.Println("c.mapjobs", c.mapJobs)

	istheremap := false
	istherereduce := false

	for _, status := range c.mapStatus {
		if status != int(Completed) {
			istheremap = true
			break
		}
	}

	for _, status := range c.reduceStatus {
		if status != int(Completed) {
			istherereduce = true
			break
		}
	}

	// fmt.Println("map statuses", c.mapStatus, istheremap)

	if istheremap {
		// fmt.Println("assigning map")
		for file, status := range c.mapStatus {
			if status == int(Pending) {
				// fmt.Println("MapID", c.fileToMapId[file])
				reply.FileName = file
				reply.MapId = c.fileToMapId[file]
				reply.NReduce = c.nReduce
				reply.IsMapJob = true
				//mark map job in progress
				c.mapStatus[file] = int(InProgress)
				fmt.Println("map assigned", reply.MapId)
				go c.CheckJobTimeout(true, false, reply.FileName, 0)
				break
			} 
			// else {
			// 	fmt.Println("no map")
			// }

		}
	} else if istherereduce {
		// fmt.Println("assigning reduce")

		for job, status := range c.reduceStatus {
			if status == int(Pending) {
				reply.IsReduceJob = true
				reply.ReduceId = job
				reply.NReduce = c.nReduce
				reply.MMaps = c.nMaps
				c.reduceStatus[job] = int(InProgress)
				go c.CheckJobTimeout(false, true, "", reply.ReduceId)
				// fmt.Println("reduce assigned")

				break
			} 
			// else {
			// 	fmt.Println("no reduce")
			// }
		}

	} else {
		// fmt.Println("all jobs done")
		reply.JobsDone = true
	}
	c.mu.Unlock()

	// go c.CheckJobTimeout(reply)
	return nil
}

func (c *Coordinator) JobCompleted(args *CompletedJob, reply *CompletedJobReply) error {
	c.mu.Lock()
	// fmt.Println("Complete job", args)
	if args.IsMapJob {
		file := args.FileName
		// if c.mapStatus[file] == int(InProgress) && c.mapJobs > 0 {

		c.mapStatus[file] = int(Completed)
		// fmt.Println("map statuses", c.mapStatus)
		// }
	} else if args.IsReduceJob {
		reduceJob := args.ReduceId
		// if c.reduceStatus[reduceJob] == int(InProgress) && c.reduceJobs > 0 {
		c.reduceStatus[reduceJob] = int(Completed)
		// }
	}

	c.mu.Unlock()
	return nil
}

func (c *Coordinator) CheckJobTimeout(mapjob bool, reducejob bool, file string, reduceId int) {
	// fmt.Println("CJT__before")

	time.Sleep(time.Second * 10)
	c.mu.Lock()
	defer c.mu.Unlock()

	fmt.Println("timing out tasks, checking...", mapjob, reducejob, file, reduceId)
	if mapjob {
		// fmt.Println("CJT__file", file)
		if c.mapStatus[file] == int(InProgress) {
			c.mapStatus[file] = int(Pending)
			// fmt.Println("one more map job added")
		} 
		// else {
		// 	fmt.Println("no in progress map tasks")
		// }
	} else if reducejob {
		// fmt.Println("CJT__reduceId", reduceId)
		if c.reduceStatus[reduceId] == int(InProgress) {
			c.reduceStatus[reduceId] = int(Pending)
			// fmt.Println("one more reduce job added")
		} 
		// else {
		// 	fmt.Println("no in progress reduce tasks")
		// }
	}

	// fmt.Println("from completed")
	// return

	// defer timer.Stop()

	// for {
	// 	select {
	// 	case <-timer.C:

	// 	default:
	// 		if args.IsMapJob {
	// 			file := args.FileName
	// 			// fmt.Println("CJT__file", file)
	// 			if c.mapStatus[file] == int(Completed) {
	// 				return
	// 			}
	// 		} else if args.IsReduceJob {
	// 			reduceId := args.ReduceId
	// 			// fmt.Println("CJT__reduceId", reduceId)
	// 			if c.reduceStatus[reduceId] == int(Completed) {
	// 				return
	// 			}
	// 		}
	// 		// return
	// 	}
	// }
	// // fmt.Println("CJT__after")

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

	istheremap := false
	istherereduce := false

	for _, status := range c.mapStatus {
		if status != int(Completed) {
			istheremap = true
			break
		}
	}

	for _, status := range c.reduceStatus {
		if status != int(Completed) {
			istherereduce = true
			break
		}
	}

	ret := !(istheremap || istherereduce)

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
