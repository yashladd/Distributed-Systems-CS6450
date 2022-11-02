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

	"math"
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

type LogEntry struct {
	Term    int
	Command interface{}
}

type Ticker struct {
	lastHeatbeatTime time.Time
	electionTimeout  int
}

func (t *Ticker) hasTimedOut() bool {
	return time.Since(t.lastHeatbeatTime).Milliseconds() > int64(t.electionTimeout)
}

func (t *Ticker) reset() {
	rand.Seed(time.Now().UnixNano())
	t.lastHeatbeatTime = time.Now()
	t.electionTimeout = rand.Intn(100) + rand.Intn(100) + 200
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
	currentTerm int        //latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        //candidateId that received vote in current term (or null if none)
	log         []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// Volatile state on all servers
	state           int // Follower/Candidate/Leader
	commitIndex     int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied     int //index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	heartbeatTicker Ticker
	applyCh         chan ApplyMsg

	nextIndex  []int //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

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
	Term         int        //leader’s term
	LeaderId     int        //so follower can redirect clients
	PrevLogIndex int        //index of log entry immediately preceding new ones
	PrevLogTerm  int        //term of prevLogIndex entry
	Entries      []LogEntry //log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        //leader’s commitIndex
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
	// TODO: write a function for this
	myLastLogTerm := rf.log[len(rf.log)-1].Term
	isTermGreater := args.LastLogTerm > myLastLogTerm
	isTermEqual := args.LastLogTerm == myLastLogTerm
	isLogLonger := args.LastLogIndex >= (len(rf.log) - 1)
	if (rf.votedFor < 0 || rf.votedFor == args.CandidateId) && (isTermGreater || isTermEqual && isLogLonger) {
		DPrintf("raft%v vote for raft%v\n", rf.me, args.CandidateId)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.heartbeatTicker.reset()
		reply.Term = rf.currentTerm
		rf.state = int(Follower)
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
	// Your code here (2B).
	rf.mu.Lock()

	term, index := -1, -1

	isleader := rf.state == int(Leader)

	if !isleader {
		rf.mu.Unlock()
		return index, term, false
	}

	term = rf.currentTerm
	rf.log = append(rf.log, LogEntry{Term: term, Command: command})
	index = len(rf.log) - 1

	rf.mu.Unlock()

	go rf.handleAppendEntries()
	return index, term, true
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
	rf.heartbeatTicker.reset()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("leader[raft%v][term:%v] beat term:%v [raft%v][%v]\n", args.LeaderId, args.Term, rf.currentTerm, rf.me, rf.state)
	DPrintf("Leader %d term %d append raft[%d]term[%d]state[%d]", args.LeaderId, args.Term, rf.me, rf.currentTerm, rf.state)
	// fmt.Printf("Raft[%v] recv appent with args => %v mylog => %v len=%v\n", rf.me, args, rf.log, len(rf.log))
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	reply.Success, reply.Term = false, rf.currentTerm

	rf.heartbeatTicker.reset()

	if args.Term > rf.currentTerm {
		rf.switchToFollower(args.Term)
	}

	prevIndex := len(rf.log) - 1

	if prevIndex < args.PrevLogIndex {
		// fmt.Printf("Server[%v] prevIdx smaller, mylog[%v], cmtidx[%d]\n", rf.me, rf.log, rf.commitIndex)
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.log = rf.log[:prevIndex]
		// fmt.Printf("Server[%x]Term not eq app, mylog[%v] cmtidx[%d]\n", rf.me, rf.log, rf.commitIndex)
		return
	}

	i := args.PrevLogIndex + 1
	j := 0

	for ; i < len(rf.log) && j < len(args.Entries); i, j = i+1, j+1 {
		if rf.log[i].Term != args.Entries[j].Term {
			break
		}
	}

	rf.log = rf.log[:i]
	entries := args.Entries[j:]

	// fmt.Printf("Applying entries Raft[%d]. My log=>%v entries%v\n", rf.me, rf.log, entries)

	if len(entries) > 0 {
		rf.log = append(rf.log, entries...)
	}

	if args.LeaderCommit > rf.commitIndex {
		minIndex := int(math.Min(float64(args.LeaderCommit), float64(len(rf.log)-1)))
		// fmt.Println("follower applying", minIndex)

		rf.commitIndex = minIndex
		if rf.commitIndex > rf.lastApplied {
			go rf.appyLogEntries()
		}
	}

	reply.Success = true

}

func (rf *Raft) handleAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	me := rf.me
	// fmt.Printf("Leader[%v] log %v\n", rf.me, rf.log)

	for peer := range rf.peers {
		if peer != me {
			args := AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			// fmt.Print("ae ", rf.me, rf.log, rf.nextIndex, rf.matchIndex)
			args.PrevLogIndex = rf.nextIndex[peer] - 1
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			args.Entries = rf.log[args.PrevLogIndex+1:]
			args.LeaderCommit = rf.commitIndex
			reply := AppendEntriesReply{}
			// if prevLogLen != len(rf.log) {

			// fmt.Printf("Sending append to raft[%v], with args[%v]\n", peer, args)
			// }

			go func(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
				if rf.sendAppendEntries(peer, args, reply) {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.switchToFollower(reply.Term)
					}

					if reply.Success {
						rf.nextIndex[peer] = args.PrevLogIndex + len(args.Entries) + 1
						rf.matchIndex[peer] = rf.nextIndex[peer] - 1
					} else {
						// fmt.Println("Not success")
						currNextIdx := rf.nextIndex[peer]
						if currNextIdx-1 >= 1 {
							rf.nextIndex[peer] = currNextIdx - 1
						}
					}

					nextCommitIndex := rf.getMaxCommitIdx()
					shouldCommit := nextCommitIndex > rf.commitIndex

					if shouldCommit {
						rf.commitIndex = nextCommitIndex
						if rf.commitIndex > rf.lastApplied {
							go rf.appyLogEntries()
						}
					}

					rf.mu.Unlock()
				}
			}(peer, &args, &reply)
		}
	}
}

func (rf *Raft) appyLogEntries() {
	rf.mu.Lock()
	commitIndex := rf.commitIndex
	lastCommitIndex := rf.lastApplied
	mylogLen := len(rf.log)
	rf.mu.Unlock()

	for i := lastCommitIndex + 1; i <= commitIndex && i < mylogLen; i++ {
		rf.mu.Lock()
		msg := ApplyMsg{true, rf.log[i].Command, i, false, []byte{}, 0, 0}
		rf.mu.Unlock()

		rf.applyCh <- msg
	}

	rf.mu.Lock()
	rf.lastApplied = commitIndex
	rf.mu.Unlock()
}

func (rf *Raft) checkForCommit(nextIdx int) bool {
	n := 1
	for i := 0; i < len(rf.matchIndex); i++ {
		if i != rf.me && rf.matchIndex[i] >= nextIdx {
			n += 1
		}
	}
	return n > len(rf.matchIndex)/2
}

func (rf *Raft) getMaxCommitIdx() int {
	maxIdx := rf.commitIndex
	nextIdx := maxIdx + 1
	for nextIdx < len(rf.log) && rf.checkForCommit(nextIdx) {
		if rf.log[nextIdx].Term == rf.currentTerm {
			maxIdx = nextIdx
		}
		nextIdx += 1
	}
	return maxIdx
}

func (rf *Raft) sendHeartbeats() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != int(Leader) {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		rf.handleAppendEntries()
		time.Sleep(HeartbeatTime * time.Millisecond)
	}
}

func (rf *Raft) handleRequestVote(peer int, term int) bool {
	rf.mu.Lock()
	args := RequestVoteArgs{}
	args.CandidateId = rf.me
	args.Term = term
	args.LastLogIndex = len(rf.log) - 1
	args.LastLogTerm = rf.log[args.LastLogIndex].Term
	rf.mu.Unlock()
	reply := RequestVoteReply{}

	ok := rf.sendRequestVote(peer, &args, &reply)

	if ok {
		rf.mu.Lock()
		if rf.currentTerm < reply.Term {
			rf.switchToFollower(reply.Term)
		}
		rf.mu.Unlock()
		return reply.VoteGranted
	} else {
		return false
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.state = int(Candidate)
	rf.currentTerm += 1
	rf.votedFor = rf.me
	me := rf.me
	term := rf.currentTerm
	rf.heartbeatTicker.reset()
	rf.mu.Unlock()

	votesRecv := 1

	for peer := range rf.peers {
		if peer != me {
			go func(peer int) {
				voteGranted := rf.handleRequestVote(peer, term)
				if voteGranted {
					rf.mu.Lock()
					votesRecv += 1
					if votesRecv > len(rf.peers)/2 {
						if rf.currentTerm == term {
							rf.state = int(Leader)
							rf.nextIndex = make([]int, len(rf.peers))
							for i := 0; i < len(rf.nextIndex); i++ {
								rf.nextIndex[i] = len(rf.log)
							}
							rf.matchIndex = make([]int, len(rf.peers))
							for i := 0; i < len(rf.matchIndex); i++ {
								rf.matchIndex[i] = 0
							}
							// fmt.Printf("Init Leader[%v], ni %v mi %v", rf.me, rf.nextIndex, rf.matchIndex)

							rf.mu.Unlock()
							go rf.sendHeartbeats()
						} else {
							rf.mu.Unlock()
						}
					} else {
						rf.mu.Unlock()
					}
				}
			}(peer)
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		state := rf.state
		hastimedOut := rf.heartbeatTicker.hasTimedOut()
		rf.mu.Unlock()
		if state != int(Leader) && hastimedOut {
			go rf.startElection()
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) generateRandom() int {
	// rand.Seed(time.Now().UnixNano())
	// return rand.Intn(HeartbeatTime) + 300
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(100) + rand.Intn(100) + 200
}

// func (rf *Raft) getLogTerm(index int) int {
// 	return rf.log[index-1].Term
// }

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
	// rf.electionTimeout = rf.generateRandom()
	rf.heartbeatTicker = Ticker{}
	rf.heartbeatTicker.reset()
	// rf.lastHeatbeatTime = time.Now()
	rf.applyCh = applyCh
	rf.log = append(rf.log, LogEntry{Term: 0, Command: nil})
	// rf.voteCh = make(chan bool)

	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	DPrintf("Starting")

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
