package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader      int
	clerkId     int64
	operationId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clerkId = nrand()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	opId := ck.operationId + 1
	var args GetArgs
	args.Key = key
	args.OpId = opId
	args.ClerkId = ck.clerkId
	value := ""

	for {
		var reply GetReply
		ok := ck.servers[ck.leader].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err != OK {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			continue
		}
		value = reply.Value
		return value
	}

}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	opId := ck.operationId + 1
	ck.operationId = opId
	var args PutAppendArgs
	args.Key = key
	args.Value = value
	args.Op = op
	args.OpId = opId
	args.ClerkId = ck.clerkId

	for {
		var reply PutAppendReply
		ok := ck.servers[ck.leader].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err != OK {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			continue
		}
		return
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
