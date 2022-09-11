package mr

import (
	// "fmt"
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
	// fmt.Println("c.mapjobs", c.mapJobs)

	pendingMapJobs , pendingReduceJobs := c.checkPendingMapReduceJobs()
	// fmt.Println("map statuses", c.mapStatus, pendingMapJobs)
	c.mu.Lock()
	if pendingMapJobs {
		// fmt.Println("assigning map")
		for file, status := range c.mapStatus {
			if status == int(Pending) {
				reply.FileName = file
				reply.MapId = c.fileToMapId[file]
				reply.NReduce = c.nReduce
				reply.IsMapJob = true
				//mark map job in progress
				c.mapStatus[file] = int(InProgress)
				// fmt.Println("map assigned", reply.MapId)
				go c.CheckJobTimeout(true, false, reply.FileName, 0)
				break
			} 
			// else {
			// 	fmt.Println("no map")
			// }

		}
	} else if pendingReduceJobs {
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
		c.mapStatus[file] = int(Completed)
		// fmt.Println("map statuses", c.mapStatus)
	} else if args.IsReduceJob {
		reduceJob := args.ReduceId
		c.reduceStatus[reduceJob] = int(Completed)
	}

	c.mu.Unlock()
	return nil
}

func (c *Coordinator) CheckJobTimeout(isMapJob bool, isReduceJob bool, file string, reduceId int) {

	time.Sleep(time.Second * 10)
	c.mu.Lock()
	defer c.mu.Unlock()

	// fmt.Println("timing out tasks, checking...", isMapJob, isReduceJob, file, reduceId)
	if isMapJob {
		if c.mapStatus[file] == int(InProgress) {
			c.mapStatus[file] = int(Pending)
			// fmt.Println("one more map job added")
		} 
		// else {
		// 	fmt.Println("no in progress map tasks")
		// }
	} else if isReduceJob {
		if c.reduceStatus[reduceId] == int(InProgress) {
			c.reduceStatus[reduceId] = int(Pending)
			// fmt.Println("one more reduce job added")
		} 
		// else {
		// 	fmt.Println("no in progress reduce tasks")
		// }
	}

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
	// c.mu.Lock()

	// istheremap := false
	// istherereduce := false

	// for _, status := range c.mapStatus {
	// 	if status != int(Completed) {
	// 		istheremap = true
	// 		break
	// 	}
	// }

	// for _, status := range c.reduceStatus {
	// 	if status != int(Completed) {
	// 		istherereduce = true
	// 		break
	// 	}
	// }
	// c.mu.Unlock()

	pendingMapJobs , pendingReduceJobs := c.checkPendingMapReduceJobs()


	ret := !(pendingMapJobs || pendingReduceJobs)


	return ret
}

func (c *Coordinator) checkPendingMapReduceJobs() (bool, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// fmt.Println("c.mapjobs", c.mapJobs)

	pendingMapJobs := false
	pendingReduceJobs := false

	for _, status := range c.mapStatus {
		if status != int(Completed) {
			pendingMapJobs = true
			break
		}
	}

	for _, status := range c.reduceStatus {
		if status != int(Completed) {
			pendingReduceJobs = true
			break
		}
	}
	return pendingMapJobs, pendingReduceJobs
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

	c.server()
	return &c
}
