package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"

	// "sort"
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// for {
	// }
	// Your worker implementation here.

	for {

		args := RequestJob{}
		reply := RequestJobReply{}

		ret := call("Coordinator.AllocateJob", &args, &reply)

		if !ret {
			log.Fatalf("Error connecting to master")
			// TODO: Is return required?
			// return
		}

		// TODO: terminating condition
		// TODO: Worker timeout condition 10 secs

		if reply.IsMapJob {
			nReduce := reply.NReduce
			mapId := reply.MapId
			fileName := reply.FileName
			PerformMap(fileName, mapf, mapId, nReduce)
			NotifyMapJobCompleted(fileName)
		}

	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func PerformMap(fileName string, mapf func(string, string) []KeyValue, mapId, nReduce int) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("Could not open file %v\n", file)
	}
	content, err := ioutil.ReadAll(file)
	// logError(err, "Could not read file %v\n", file)
	if err != nil {
		log.Fatalf("Could not read file %v\n", file)
	}
	file.Close()
	kva := mapf(fileName, string(content))

	mapFiles := make([]*os.File, 0, nReduce)

	// path, err := os.Getwd()
	// if err != nil {
	// 	log.Println("Couldn't get corrent working dir", err)
	// }
	// currentDir := path + "/"

	for i := 0; i < nReduce; i++ {
		file, _ := os.Create("mr-" + strconv.Itoa(mapId) + "-" + strconv.Itoa(i))
		mapFiles = append(mapFiles, file)
	}

	// sort.Sort(ByKey(kva))

	for _, kv := range kva {
		i := ihash(kv.Key) % nReduce
		// jsonKV, _ := json.MarshalIndent(kv, "", "")
		enc := json.NewEncoder(mapFiles[i])
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("Could not encode key value pair ->  %v %v", kv.Key, kv.Value)
		}
	}

	for i := 0; i < nReduce; i++ {
		mapFiles[i].Close()
	}
}

func NotifyMapJobCompleted(fileName string) {
	args := CompletedJob{}
	args.IsMapJob = true
	args.FileName = fileName
	reply := CompletedJobReply{}
	ret := call("Coordinator.JobCompleted", &args, &reply)
	if !ret {
		log.Fatal("Errror sending complted map status to coordinator")
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
