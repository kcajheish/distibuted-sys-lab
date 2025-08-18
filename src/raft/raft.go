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

	"context"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh chan ApplyMsg

	currentTerm int
	voteFor     int

	logs        []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	timeout     time.Time
	votes       int

	status int // Leader, Candidate, Follower
}

type LogEntry struct {
	Command interface{}
	Term    int
}

const (
	LEADER = iota
	FOLLOWER
	CANDIDATE
)

const NO_LEADER = -1

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.status == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// restore previously persisted state.
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	///
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	CurrentTerm int
	Success     bool
}

func getRandomTimeout() time.Time {
	max_t := 800
	min_t := 300
	duration := time.Duration(rand.Intn(max_t-min_t+1)+min_t) * time.Millisecond
	return time.Now().Add(duration)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term { // Follower spots a stale leader.
		reply.CurrentTerm = rf.currentTerm
		reply.Success = false
		return
	}

	rf.timeout = getRandomTimeout()

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.status = FOLLOWER
		rf.voteFor = args.LeaderId
	}

	if len(args.Entries) > 0 {
		// check logs are up to date
		if args.PrevLogIndex >= len(rf.logs) || (args.PrevLogIndex > 0 && rf.logs[args.PrevLogIndex].Term != args.Term) {
			reply.Success = false
			log.Printf("s%d logs are more up to date than leader; log=%v, args=%v", rf.me, rf.logs, args)
			return
		}

		currentIndex := args.PrevLogIndex + 1
		if currentIndex < len(rf.logs) {
			if rf.logs[currentIndex].Term != args.Term {
				rf.logs = rf.logs[:currentIndex]
			}
		}

		rf.logs = append(rf.logs, args.Entries...)
	}

	// log.Printf("arg_logs=%v logs=%v leader_commit=%d commit=%d\n", args.Entries, rf.logs, args.LeaderCommit, rf.commitIndex)

	lastIndex := len(rf.logs) - 1

	prevCommit := rf.commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(lastIndex, args.LeaderCommit)
	}

	if prevCommit != rf.commitIndex {
		log.Printf("s%d commit from %d to %d", rf.me, prevCommit+1, rf.commitIndex)
		go rf.ApplyCommand(prevCommit+1, rf.commitIndex)
	}

	reply.Success = true
	reply.CurrentTerm = rf.currentTerm

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if rf.currentTerm < args.Term || rf.voteFor == NO_LEADER || rf.voteFor == args.CandidateId {
		rf.timeout = getRandomTimeout()
		rf.currentTerm = args.Term
		if rf.status == LEADER {
			Debug(dInfo, "AppendEntries: S%d becomes follower", rf.me)
		}
		rf.status = FOLLOWER
		rf.voteFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		return
	}

	// follower grants vote to other candidate
	reply.VoteGranted = false
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) { // index, term, is_leader
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := rf.currentTerm
	isLeader := rf.status == LEADER
	if isLeader {
		entry := LogEntry{
			Command: command,
			Term:    rf.currentTerm,
		}
		index = len(rf.logs)
		rf.logs = append(rf.logs, entry)
		log.Printf("s%d receives cmd=%v at index=%d\n", rf.me, command, index)
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

var heartbeatInterval time.Duration = time.Duration(100 * time.Millisecond)

func (rf *Raft) heartbeats() {
	for !rf.killed() {

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			rf.mu.Lock()
			if rf.status != LEADER {
				rf.mu.Unlock()
				break
			}

			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.commitIndex,
			}
			reply := AppendEntriesReply{}
			batchSize := 10
			logs := rf.getLogsInBatch(rf.nextIndex[i], batchSize)

			if logs != nil {
				args.Entries = append(args.Entries, logs...)
				args.PrevLogIndex = rf.nextIndex[i] - 1

			}
			rf.mu.Unlock()
			go func() {
				if rf.sendAppendEntries(i, &args, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					log.Printf("heartbeats: s%d s%d;  args=%#v reply=%v\n", rf.me, i, args, reply)

					if !reply.Success && reply.CurrentTerm > rf.currentTerm {
						rf.currentTerm = reply.CurrentTerm
						rf.voteFor = NO_LEADER
						rf.status = FOLLOWER
						Debug(dInfo, "Stale leader S%d becomes follower", rf.me)
						return
					}
					if logs != nil {
						log.Printf("s%d sends logs to s%d from %d to %d\n", rf.me, i, rf.nextIndex[i], rf.nextIndex[i]+len(logs)-1)
						rf.logMatching(reply.Success, i, len(logs))
					}

					if reply.Success && len(logs) > 0 {
						rf.updateCommitIndex()
					}

				}
			}()
		}
		time.Sleep(heartbeatInterval)
	}
}

func (rf *Raft) getLogsInBatch(from int, size int) []LogEntry {
	if from >= len(rf.logs) {
		return nil
	}

	end := from + min(size, len(rf.logs)-from)
	return rf.logs[from:end]
}

func (rf *Raft) logMatching(status bool, server int, size int) {
	if status {
		rf.nextIndex[server] += size
		rf.matchIndex[server] = rf.nextIndex[server] - 1

	} else {
		rf.nextIndex[server] -= 1
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	}
}

func (rf *Raft) updateCommitIndex() {
	if rf.status != LEADER {
		return
	}

	old := rf.commitIndex
	for index := rf.commitIndex + 1; index < len(rf.logs); index++ {
		if rf.logs[index].Term != rf.currentTerm {
			continue
		}
		count := 1
		for p := 0; p < len(rf.peers); p++ {
			if p == rf.me {
				continue
			}
			if rf.matchIndex[p] >= index {
				count += 1
			}
		}

		if count >= rf.Majority() {
			rf.commitIndex = index
		}
	}
	if old != rf.commitIndex {
		log.Printf("s%d(leader) commit index from=%d to=%d match_index=%#v\n", rf.me, old+1, rf.commitIndex, rf.matchIndex)
		go rf.ApplyCommand(old+1, rf.commitIndex)
	}
}

func (rf *Raft) ApplyCommand(from int, to int) {
	for i := from; i <= to; i++ {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			CommandIndex: i,
			Command:      rf.logs[i].Command,
		}
	}
}

func (rf *Raft) Majority() int {
	return len(rf.peers)/2 + 1
}

func (rf *Raft) AskVote(ctx context.Context, wg *sync.WaitGroup, server int, candidate int, term int, out chan int) {
	defer wg.Done()
	args := &RequestVoteArgs{
		Term:        term,
		CandidateId: candidate,
	}
	reply := &RequestVoteReply{}

	ok := false
	for !ok {
		select {
		case <-ctx.Done():
			out <- 0
			return
		default:
			ok = rf.sendRequestVote(server, args, reply)
		}
	}
	log.Printf("RequestVote: s%d args=%#v reply=%#v", rf.me, args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status != CANDIDATE || rf.currentTerm != term {
		out <- -1
		Debug(dVote, "S%d not a candidate anymore; cancel vote", rf.me)
	} else if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.voteFor = NO_LEADER
		rf.status = FOLLOWER
		out <- -1
		Debug(dVote, "S%d term is greater than S%d; S%d returns to follower", server, rf.me, rf.me)
	} else if reply.VoteGranted {
		Debug(dVote, "S%d grant vote to S%d", server, rf.me)
		out <- 1
	} else {
		out <- 0
		Debug(dVote, "S%d reject vote to S%d", server, rf.me)

	}

}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.status == FOLLOWER && rf.timeout.Before(time.Now()) {
			rf.currentTerm += 1
			rf.voteFor = rf.me
			rf.status = CANDIDATE
			rf.timeout = getRandomTimeout()
			Debug(dTimer, "S%d starts election with term %d", rf.me, rf.currentTerm)
			var wg sync.WaitGroup
			wg.Add(len(rf.peers) - 1)
			ctx, cancel := context.WithCancel(context.Background())
			out := make(chan int, len(rf.peers))
			candidateId := rf.me
			curTerm := rf.currentTerm
			rf.mu.Unlock()
			for server := 0; server < len(rf.peers); server++ {
				if server == rf.me {
					continue
				}
				go rf.AskVote(ctx, &wg, server, candidateId, curTerm, out)
			}
			n := 1
			votes := 1
			run := true
			for rf.timeout.After(time.Now()) && run {
				select {
				case v := <-out:
					rf.mu.Lock()
					if v == -1 || rf.status != CANDIDATE {
						// return to follower
						run = false
					} else {
						n += 1
						votes += v
						if votes >= rf.Majority() {
							rf.status = LEADER
							Debug(dLeader, "S%d wins election", rf.me)
							run = false
						} else if n-votes >= rf.Majority() {
							rf.status = FOLLOWER
							Debug(dLeader, "S%d lose election in term %d", rf.me, rf.currentTerm)
							run = false
						}
					}
					rf.mu.Unlock()
				default:
					// do nothing
				}
			}
			cancel()
			wg.Wait()
			close(out)
			if rf.status != LEADER {
				rf.status = FOLLOWER
			}
			Debug(dVote, "S%d get %d votes in term %d", rf.me, votes, rf.currentTerm)
		} else {
			rf.mu.Unlock()
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.status = FOLLOWER
	rf.voteFor = NO_LEADER
	rf.timeout = getRandomTimeout()
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	size := len(peers)
	rf.nextIndex = make([]int, size)
	rf.matchIndex = make([]int, size)
	for i := 0; i < size; i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}
	rf.logs = make([]LogEntry, 1, 100)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeats()

	return rf
}
