package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"time"

	"math/rand"

	"6.824/labrpc"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
	HeartbeatTime = 100
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state
	currentTerm int //latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int //candidateId that received vote in current term (or null if none)
	// log         []*LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// Volatile state on all servers
	state       int // Follower/Candidate/Leader
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int //index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	lastHeatbeatTime time.Time
	electionTimeout  int
	// voteCh           chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == int(Leader)
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //candidate’s term
	CandidateId  int //candidate requesting vote
	LastLogIndex int //index of candidate’s last log entry
	LastLogTerm  int //term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int //leader’s term
	LeaderId     int //so follower can redirect clients
	PrevLogIndex int //index of log entry immediately preceding new ones
	PrevLogTerm  int //term of prevLogIndex entry
	// Entries      []LogEntry //log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int //leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  //currentTerm, for leader to update itself
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Candidate[raft%v][term:%v] request vote: raft[%v]state[%v]term[%v]\n", args.CandidateId, args.Term, rf.me, rf.state, rf.currentTerm)

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	reply.Term = rf.currentTerm

	if args.Term > rf.currentTerm {
		DPrintf("raft[%d], Switching to follower bigger term received", rf.me)
		rf.switchToFollower(args.Term)
	}

	//|| rf.votedFor == args.CandidateId

	if rf.votedFor == -1 {
		DPrintf("raft%v vote for raft%v\n", rf.me, args.CandidateId)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.lastHeatbeatTime = time.Now()
		reply.Term = rf.currentTerm
	}

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) switchToFollower(term int) {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	rf.state = int(Follower)
	rf.currentTerm = term
	rf.votedFor = -1
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("leader[raft%v][term:%v] beat term:%v [raft%v][%v]\n", args.LeaderId, args.Term, rf.currentTerm, rf.me, rf.state)
	DPrintf("Leader %d term %d beat raft[%d]term[%d]state[%d]", args.LeaderId, args.Term, rf.me, rf.currentTerm, rf.state)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		// reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.switchToFollower(args.Term)
	}

	// reply.Success = true
	rf.lastHeatbeatTime = time.Now()
}

func (rf *Raft) sendHeartbeats() {
	for peer := range rf.peers {
		if peer != rf.me {
			go func(peer int) {
				args := AppendEntriesArgs{}
				rf.mu.Lock()
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				rf.mu.Unlock()
				reply := AppendEntriesReply{}
				if rf.sendAppendEntries(peer, &args, &reply) {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.switchToFollower(reply.Term)
					}
					rf.mu.Unlock()
				}
			}(peer)
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		state := rf.state
		me := rf.me
		timeoutDuration := rf.electionTimeout

		rf.mu.Unlock()

		if state == int(Leader) {
			rf.mu.Lock()
			DPrintf("Leader ---> raft[%d]term[%d], electiontimeout[%d]", me, rf.currentTerm, timeoutDuration)
			rf.mu.Unlock()
			rf.sendHeartbeats()
			time.Sleep(HeartbeatTime * time.Millisecond)
		} else {
			rf.mu.Lock()
			DPrintf("Follower ---> raft[%d]term[%d], electiontimeout[%d]", me, rf.currentTerm, timeoutDuration)
			rf.mu.Unlock()
			// DPrintf("raft[%d]term[%d], electiontimeout[%d]", me, rf.currentTerm, timeoutDuration)

			time.Sleep(time.Duration(timeoutDuration) * time.Millisecond)
			rf.mu.Lock()
			// lastHearRec := rf.lastHeatbeatTime
			DPrintf("raft[%d] Time since last heart %d", rf.me, time.Since(rf.lastHeatbeatTime).Milliseconds())
			startElection := time.Since(rf.lastHeatbeatTime).Milliseconds() > HeartbeatTime
			rf.mu.Unlock()

			if startElection {
				rf.mu.Lock()
				rf.state = int(Candidate)
				rf.currentTerm += 1
				rf.votedFor = rf.me
				args := RequestVoteArgs{}
				args.Term = rf.currentTerm
				args.CandidateId = rf.me
				rf.mu.Unlock()

				voteCh := make(chan bool)
				var voteMu sync.Mutex
				voteCount := 1
				for peer := range rf.peers {
					if peer != me {
						go func(peer int) {
							reply := RequestVoteReply{}
							ok := rf.sendRequestVote(peer, &args, &reply)
							if !ok {
								voteCh <- true
								return
							}
							// Check if it should be !=
							rf.mu.Lock()
							if reply.Term > rf.currentTerm {
								rf.switchToFollower(reply.Term)
							} else if reply.VoteGranted {
								voteMu.Lock()
								voteCount = voteCount + 1
								// if voteCount > len(rf.peers)/2 {
								// 	rf.state = int(Leader)
								// }
								voteMu.Unlock()
							}
							rf.mu.Unlock()
							DPrintf("Sending %d", peer)
							voteCh <- true
						}(peer)
					}
				}

				for i := 0; i < len(rf.peers)-1; i++ {
					DPrintf("Waiting %d", i)
					<-voteCh
					DPrintf("Received %d", i)
					rf.mu.Lock()
					// if rf.state == int(Candidate) {
					voteMu.Lock()
					if voteCount > len(rf.peers)/2 {
						rf.state = int(Leader)
						rf.mu.Unlock()
						voteMu.Unlock()
						break
					}
					voteMu.Unlock()
					// }
					rf.mu.Unlock()
				}

			}
			rf.mu.Lock()
			rf.electionTimeout = rf.generateRandom()
			rf.mu.Unlock()
		}

	}
}

func (rf *Raft) generateRandom() int {
	// rand.Seed(time.Now().UnixNano())
	// return rand.Intn(HeartbeatTime) + 300
	rand.Seed(time.Now().UnixNano())
	return (rand.Intn(200)) + rand.Intn(100) + 200
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = int(Follower)
	rf.votedFor = -1
	rf.electionTimeout = rf.generateRandom()
	rf.lastHeatbeatTime = time.Now()
	// rf.voteCh = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
