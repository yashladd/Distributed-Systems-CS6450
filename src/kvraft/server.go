package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// type Optype int

// const (
// 	putappend Optype = iota
// 	get
// )

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType  string
	Key     string
	Value   string
	ClerkId int64
	OpId    int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	database map[string]string
	opDone   map[int]chan Op
	seen     map[int64]int
}

func (kv *KVServer) tryToApply(operation Op) (bool, Op) {
	idx, _, isLeader := kv.rf.Start(operation)

	if !isLeader {
		return false, operation
	}

	kv.mu.Lock()

	if _, ok := kv.opDone[idx]; !ok {
		kv.opDone[idx] = make(chan Op, 1)
	}
	channel := kv.opDone[idx]

	kv.mu.Unlock()

	select {
	case opApplied := <-channel:
		isDuplicated := operation.ClerkId == opApplied.ClerkId && operation.OpId == opApplied.OpId
		return isDuplicated, opApplied
	case <-time.After(500 * time.Millisecond):
		return false, operation
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	// DPrintf("KV [%v] received GET with args: %v", kv.me, args)
	kv.mu.Unlock()
	var operation Op
	operation.OpType = "Get"
	operation.Key = args.Key
	operation.OpId = args.OpId
	operation.ClerkId = args.ClerkId

	ok, opApplied := kv.tryToApply(operation)

	if ok {
		reply.Err = OK
		reply.Value = opApplied.Value
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	// DPrintf("KV [%v] received PutAppend with args: %v", kv.me, args)
	kv.mu.Unlock()
	var operation Op
	operation.OpType = args.Op
	operation.Key = args.Key
	operation.Value = args.Value
	operation.OpId = args.OpId
	operation.ClerkId = args.ClerkId

	ok, _ := kv.tryToApply(operation)

	// DPrintf("KV [%v] OpId: %v, ApplyStatus %v", kv.me, args.OpId, ok)

	if ok {
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.database = make(map[string]string)
	kv.opDone = make(map[int]chan Op)
	kv.seen = make(map[int64]int)

	go func() {
		for applyMsg := range kv.applyCh {
			if applyMsg.CommandValid == false {
				continue
			}

			operation := applyMsg.Command.(Op)

			kv.mu.Lock()
			// DPrintf("KV [%d] receives reply from applyCh: %v Command: %v Operation: %v", kv.me, applyMsg, applyMsg.Command, applyMsg.Command.(Op))
			// DPrintf("Opeation: Type: %v, Key: %v, Value: %v", operation.OpType, operation.Key, operation.Value)

			if operation.OpType == "Get" {
				operation.Value = kv.database[operation.Key]
			} else {
				prevOpId, ok := kv.seen[operation.ClerkId]
				// DPrintf("KV [%v], applyCh got PutAppend prevOpId=%v, currentOpId=%v", kv.me, prevOpId, operation.OpId)
				if !ok || operation.OpId > prevOpId {
					kv.seen[operation.ClerkId] = operation.OpId
					if operation.OpType == "Put" {
						kv.database[operation.Key] = operation.Value
					} else {
						kv.database[operation.Key] = kv.database[operation.Key] + operation.Value
					}
				}
			}

			if _, ok := kv.opDone[applyMsg.CommandIndex]; !ok {
				kv.opDone[applyMsg.CommandIndex] = make(chan Op, 1)
			}
			channel := kv.opDone[applyMsg.CommandIndex]

			channel <- operation
			// DPrintf("KV [%v] applied cmd [%v] at idx [%v]", kv.me, operation, applyMsg.CommandIndex)
			kv.mu.Unlock()

		}
	}()

	return kv
}
