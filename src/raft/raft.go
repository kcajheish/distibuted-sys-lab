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

	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
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
	applyCh     chan ApplyMsg
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	timeout     time.Time
	status      int // Leader, Candidate, Follower
	Persistent
	SnapshotMeta
}

type Persistent struct {
	CurrentTerm int
	VoteFor     int
	Logs        []LogEntry
}

type SnapshotMeta struct {
	LastIncludedIndex int
	LastIncludedTerm  int
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

func (rf *Raft) StateToString() string {
	arr := []string{}
	for i, log := range rf.Logs {
		if i == 0 {
			continue
		}
		arr = append(arr, fmt.Sprintf("%d: term %d", i+rf.LastIncludedIndex, log.Term))
	}

	return fmt.Sprintf("{s%d commit_index=%d, snapshot_index=%d, snapshot_term=%d, logs=[%s]}", rf.me, rf.commitIndex, rf.LastIncludedIndex, rf.LastIncludedTerm, strings.Join(arr, ", "))
}

func (rf *Raft) StatusToString(status int) string {
	var s string
	switch status {
	case LEADER:
		s = "leader"
	case FOLLOWER:
		s = "follower"
	case CANDIDATE:
		s = "candidate"
	default:
		s = "unknown state"
	}
	return s
}

const NO_LEADER = -1

const BATCH_SIZE = 50

const MAX_LOGS = 300

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.CurrentTerm, rf.status == LEADER
}

func (rf *Raft) SerializeRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VoteFor)
	e.Encode(rf.Logs)
	e.Encode(rf.LastIncludedIndex)
	e.Encode(rf.LastIncludedTerm)
	data := w.Bytes()
	return data
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	data := rf.SerializeRaftState()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var p Persistent
	var s SnapshotMeta
	if (d.Decode(&p.CurrentTerm) != nil) || (d.Decode(&p.VoteFor) != nil) || (d.Decode(&p.Logs) != nil) || (d.Decode(&s.LastIncludedIndex) != nil) || (d.Decode(&s.LastIncludedTerm) != nil) {
		log.Fatalf("s%d can't read persist data", rf.me)
	} else {
		rf.Persistent = p
		rf.SnapshotMeta = s
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	if rf.commitIndex < index {
		panic("can't snapshot entries that haven't been committed.")
	}
	newLogs := make([]LogEntry, 1, MAX_LOGS)
	size := rf.Size() - index
	start := rf.Index(index + 1)

	rf.LastIncludedTerm = rf.LogAt(index).Term
	rf.LastIncludedIndex = index

	if size > 0 {
		// copy logs[index+1:end] to an new array
		for i := 0; i < size; i++ {
			newLogs = append(newLogs, rf.Logs[start+i])
		}
		rf.Logs = newLogs
		Debug(dClient, "s%d snapshot_index=%d logs index %d -> %d", rf.me, index, index+1, rf.Size())
	} else {
		Debug(dClient, "s%d snapshot_index=%d logs index none", rf.me, index)

	}

	if snapshot == nil {
		panic("client: snapshot can't be nil")
	}

	// persist both snapshot
	state := rf.SerializeRaftState()
	rf.persister.SaveStateAndSnapshot(state, snapshot)
}

type InstallSnapshotArgs struct {
	Term              int    // leader’s term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte //raw bytes of the entire snapshot
}

func (args *InstallSnapshotArgs) ToString() string {
	return fmt.Sprintf("{ InstallSnapshotArgs: Term=%d LeaderId=%d LastIncludedIndex=%d LastIncludedTerm=%d}", args.Term, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.LastIncludedIndex >= args.LastIncludedIndex {
		Debug(dSnap, "InstallSnapshot: s%d can't move state backward: index, %d -> %d; args=%s \n", rf.me, rf.LastIncludedIndex, args.LastIncludedIndex, args.ToString())
		return
	}

	reply.Term = rf.CurrentTerm

	// check leader's term
	if rf.CurrentTerm > args.Term {
		return
	}

	if rf.CurrentTerm < args.Term {
		rf.CurrentTerm = args.Term
		rf.status = FOLLOWER
		rf.VoteFor = NO_LEADER
		return
	}

	rf.timeout = getRandomTimeout()

	// Persist raft state and snapshot.
	// Log entries are discarded because they fail log-matching.
	// Create new slice. Allow old slice to be garbage collected.
	newLogs := make([]LogEntry, 1, MAX_LOGS)
	j := rf.Index(rf.LastIncludedIndex)
	if j > 0 && j < len(rf.Logs) && rf.Logs[j].Term == args.LastIncludedTerm {
		for i := j + 1; i < len(rf.Logs); i = i + 1 {
			newLogs = append(newLogs, rf.Logs[i])
		}
	}
	rf.Logs = newLogs

	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = rf.commitIndex
	rf.LastIncludedIndex = args.LastIncludedIndex
	rf.LastIncludedTerm = args.LastIncludedTerm
	state := rf.SerializeRaftState()
	rf.persister.SaveStateAndSnapshot(state, args.Data)
	if args.Data == nil {
		log.Panicf("snapshot can't be nil state=%s\n", rf.StateToString())
	}
	go rf.ApplySnapshot(args.Data, args.LastIncludedTerm, args.LastIncludedIndex)

}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
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
	ConflictMeta
}

type ConflictMeta struct {
	Term   int
	Index  int // index = 0 when entry doesn't exit in follower log
	Length int
}

func getRandomTimeout() time.Time {
	max_t := 800
	min_t := 500
	duration := time.Duration(rand.Intn(max_t-min_t+1)+min_t) * time.Millisecond
	return time.Now().Add(duration)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.CurrentTerm > args.Term { // Follower spots a stale leader.
		reply.CurrentTerm = rf.CurrentTerm
		reply.Success = false
		return
	}

	if rf.CurrentTerm < args.Term {
		rf.CurrentTerm = args.Term
		rf.status = FOLLOWER
		rf.VoteFor = NO_LEADER
		reply.Success = false
		return
	}

	rf.timeout = getRandomTimeout()

	isLogMatched := args.PrevLogIndex == 0 ||
		args.PrevLogIndex == rf.LastIncludedIndex && args.PrevLogTerm == rf.LastIncludedTerm ||
		(args.PrevLogIndex > rf.LastIncludedIndex && rf.Valid(args.PrevLogIndex) && (rf.LogAt(args.PrevLogIndex).Term == args.PrevLogTerm))

	if !isLogMatched {
		reply.Success = false
		if args.PrevLogIndex <= rf.LastIncludedIndex {
			reply.ConflictMeta.Term = rf.LastIncludedTerm
			reply.ConflictMeta.Index = rf.LastIncludedIndex
		} else if rf.Valid(args.PrevLogIndex) {
			reply.ConflictMeta.Term = rf.LogAt(args.PrevLogIndex).Term
			reply.ConflictMeta.Index = rf.FirstEntry(args.PrevLogIndex)
		}

		reply.ConflictMeta.Length = rf.Size()

		Debug(dLog, "s%d previous log match fails; args=%+v reply=%+v state=%s", rf.me, args, reply, rf.StateToString())
		return
	}

	Debug(dLog, "AppendEntries s%d args=%+v reply=%+v state=%s", rf.me, args, reply, rf.StateToString())

	currentIndex := args.PrevLogIndex + 1
	for i, j := currentIndex, 0; i < currentIndex+len(args.Entries); i, j = i+1, j+1 {
		if i <= rf.Size() {
			index := rf.Index(i)
			if rf.LogAt(i).Term != args.Entries[j].Term {
				rf.Logs = rf.Logs[:index+1]
			}
			rf.Logs[index] = args.Entries[j]
		} else {
			rf.Logs = append(rf.Logs, args.Entries[j])
		}
	}

	lastIndex := rf.Size()
	prevCommit := rf.commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(lastIndex, args.LeaderCommit)
	}

	if prevCommit != rf.commitIndex {
		Debug(dLog, "s%d commit from %d to %d; ", rf.me, prevCommit+1, rf.commitIndex)
		go rf.ApplyCommand()
	}

	reply.Success = true
	reply.CurrentTerm = rf.CurrentTerm

}

func (rf *Raft) Valid(index int) bool {
	return index > 0 && index <= rf.Size()
}

func (rf *Raft) FirstEntry(j int) int {
	target := rf.LogAt(j).Term
	res := j
	for i := j - 1; i > rf.LastIncludedIndex && rf.LogAt(i).Term == target; i-- {
		res = i
	}

	return res
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
	defer rf.persist()

	reply.Term = rf.CurrentTerm
	if rf.CurrentTerm > args.Term {
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		return
	}

	if rf.CurrentTerm < args.Term {
		rf.CurrentTerm = args.Term
		rf.status = FOLLOWER
		rf.VoteFor = NO_LEADER
	}

	lastTerm := -1
	lastIndex := -1
	if rf.Size() == rf.LastIncludedIndex {
		lastTerm = rf.LastIncludedTerm
		lastIndex = rf.LastIncludedIndex
	} else {
		lastEntry := rf.Logs[len(rf.Logs)-1]
		lastTerm = lastEntry.Term
		lastIndex = rf.LastIncludedIndex + len(rf.Logs) - 1
	}
	isLeaderUpToDate := (args.LastLogTerm > lastTerm) || (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)
	if !isLeaderUpToDate {
		Debug(dVote, "s%d rejects vote since leader is not up to date; args=%+v state=%s", rf.me, args, rf.StateToString())
		reply.VoteGranted = false
		return
	}

	if rf.VoteFor == NO_LEADER || rf.VoteFor == args.CandidateId {
		if rf.status == LEADER {
			Debug(dInfo, "AppendEntries: s%d becomes follower", rf.me)
		}
		rf.status = FOLLOWER
		rf.VoteFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = rf.CurrentTerm
		rf.timeout = getRandomTimeout()
		return
	}

	// follower grants vote to other candidate
	reply.VoteGranted = false
	Debug(dVote, "s%d reject votes; args=%+v reply=%+v current_term=%d vote_for=%d", rf.me, args, reply, rf.CurrentTerm, rf.VoteFor)
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
	term := rf.CurrentTerm
	isLeader := rf.status == LEADER
	if isLeader {
		entry := LogEntry{
			Command: command,
			Term:    rf.CurrentTerm,
		}
		index = rf.Size() + 1
		rf.Logs = append(rf.Logs, entry)
		rf.persist()
		Debug(dClient, "s%d receives cmd=%+v at index=%d\n", rf.me, command, index)
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

func (rf *Raft) checkState(expectedTerm int, expectedStatus int) error {
	if expectedTerm == rf.CurrentTerm && expectedStatus == rf.status {
		return nil
	}

	errMsg := "checkState fails"
	if rf.status != expectedStatus {
		errMsg = fmt.Sprintf("%s; expected_status = %d actual_status = %d", errMsg, expectedStatus, rf.status)
	}

	if rf.CurrentTerm != expectedTerm {
		errMsg = fmt.Sprintf("%s; expected_term = %d actual_term = %d", errMsg, expectedTerm, rf.CurrentTerm)
	}
	return errors.New(errMsg)
}

func (rf *Raft) heartbeats() {
	for !rf.killed() {
		rf.mu.Lock()
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}

			if rf.status != LEADER {
				break
			}

			if rf.LastIncludedIndex > 0 && rf.LastIncludedIndex >= rf.nextIndex[i] {
				Debug(dSnap, "s%d install snapshot to s%d nextIndex %d within snapshot index %d", rf.me, i, rf.nextIndex[i], rf.SnapshotMeta.LastIncludedIndex)
				args := InstallSnapshotArgs{
					Term:              rf.CurrentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.LastIncludedIndex,
					LastIncludedTerm:  rf.LastIncludedTerm,
					Data:              rf.persister.ReadSnapshot(),
				}
				reply := InstallSnapshotReply{}
				go func(i int, args InstallSnapshotArgs, reply InstallSnapshotReply) {
					if rf.sendInstallSnapshot(i, &args, &reply) {
						rf.mu.Lock()
						defer rf.mu.Unlock()

						if err := rf.checkState(args.Term, LEADER); err != nil {
							Debug(dLeader, "s%d(leader) abort InstallSnapshot; %+v", rf.me, err)
							return
						}

						// Follower's snapshot index is larger.
						// This leader might be stale.
						if reply.Term > rf.CurrentTerm {
							rf.CurrentTerm = reply.Term
							rf.VoteFor = NO_LEADER
							rf.status = FOLLOWER
							rf.persist()
							Debug(dLeader, "Stale leader s%d becomes follower", rf.me)
							return
						}

						Debug(dLeader, "install snapshot: s%d -> s%d;  args={%s} reply=%+v\n", rf.me, i, args.ToString(), reply)

						rf.matchIndex[i] = args.LastIncludedIndex
						rf.nextIndex[i] = args.LastIncludedIndex + 1
						rf.updateCommitIndex()
					}
				}(i, args, reply)

			} else {

				// if rf.nextIndex[i] <= rf.LastIncludedIndex {
				// 	log.Panicf("leader forget to install snapshot on a lagging follower")
				// }

				args := AppendEntriesArgs{
					Term:         rf.CurrentTerm,
					LeaderId:     rf.me,
					LeaderCommit: rf.commitIndex,
				}
				reply := AppendEntriesReply{}
				logs := rf.getLogsInBatch(rf.nextIndex[i], BATCH_SIZE)
				args.PrevLogIndex = rf.nextIndex[i] - 1

				// If a log of nextIndex[i] is snapshotted, call InstallSnapshot RPC first
				// can't access logs with slice index because entry is left-shifted after snapshot
				// create an interface to access an entry indirectly
				if args.PrevLogIndex == rf.LastIncludedIndex {
					args.PrevLogTerm = rf.LastIncludedTerm
				} else {
					args.PrevLogTerm = rf.LogAt(args.PrevLogIndex).Term
				}
				if logs != nil {
					args.Entries = append(args.Entries, logs...)
				}
				go func(i int, args AppendEntriesArgs, reply AppendEntriesReply) {
					if rf.sendAppendEntries(i, &args, &reply) {
						rf.mu.Lock()
						defer rf.mu.Unlock()

						Debug(dLeader, "heartbeats: s%d -> s%d;  args=%+v reply=%+v snapshot=%+v\n", rf.me, i, args, reply, rf.SnapshotMeta)

						if err := rf.checkState(args.Term, LEADER); err != nil {
							Debug(dLeader, "s%d(leader) abort AppendEntries; %+v", rf.me, err)
							return
						}

						if !reply.Success && reply.CurrentTerm > rf.CurrentTerm {
							rf.CurrentTerm = reply.CurrentTerm
							rf.VoteFor = NO_LEADER
							rf.status = FOLLOWER
							rf.persist()
							Debug(dLeader, "Stale leader s%d becomes follower", rf.me)
							return
						}

						if args.Entries != nil {
							Debug(dLog, "s%d sends logs to s%d from %d to %d\n", rf.me, i, rf.nextIndex[i], rf.nextIndex[i]+len(args.Entries)-1)
						}

						// rollback matchIndex quickly
						rf.logMatching(reply, i, args.PrevLogIndex, len(args.Entries))

						if reply.Success && len(args.Entries) > 0 {
							rf.updateCommitIndex()
						}

					}
				}(i, args, reply)
			}

		}
		rf.mu.Unlock()
		time.Sleep(heartbeatInterval)
	}
}

func (rf *Raft) Index(i int) int {
	offset := rf.SnapshotMeta.LastIncludedIndex
	if i < offset {
		log.Panicf("index can't be smaller than offset; index=%d left_offset=%d", i, offset)
	}
	return i - offset
}

func (rf *Raft) Slice(from int, end int) []LogEntry {
	from = rf.Index(from)
	end = rf.Index(end)
	return rf.Logs[from:end]
}

func (rf *Raft) LogAt(i int) LogEntry {
	return rf.Logs[rf.Index(i)]
}

// number of log entries
// snapshot entries are included
func (rf *Raft) Size() int {
	return rf.LastIncludedIndex + len(rf.Logs) - 1
}

func (rf *Raft) getLogsInBatch(from int, size int) []LogEntry {
	from = rf.Index(from)
	if from > len(rf.Logs)-1 {
		return nil
	}

	end := from + min(size, len(rf.Logs)-from)
	return rf.Logs[from:end]
}

func (rf *Raft) logMatching(reply AppendEntriesReply, server int, index int, size int) {
	// Avoid duplicate rpc call.
	// Make it idempotent.
	// index: previous log index
	if index+1 != rf.nextIndex[server] {
		return
	}

	if reply.Success {
		rf.nextIndex[server] += size
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	} else {
		// rollback nextIndex
		meta := reply.ConflictMeta
		if meta.Index == 0 { // PrevLog doesn't exist.
			rf.nextIndex[server] = meta.Length + 1
		} else {
			j := -1
			term := -1
			for i := index; i > rf.LastIncludedIndex; i-- { // always send the log which isn't snapshotted. so set last included index as lower limit
				term = rf.LogAt(i).Term
				if term == meta.Term {
					j = i
					break
				} else if term < meta.Term {
					break
				}
			}
			if j > -1 && term == meta.Term { // conflict term found
				rf.nextIndex[server] = j
			} else {
				rf.nextIndex[server] = meta.Index
			}
		}
	}
}

// only reset index when server becomes leader
func (rf *Raft) resetIndex() {
	if rf.status != LEADER {
		log.Panicf("s%d not a leader", rf.me)
	}
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = len(rf.Logs) + rf.LastIncludedIndex
	}
}

func (rf *Raft) updateCommitIndex() {
	if rf.status != LEADER {
		return
	}

	old := rf.commitIndex
	for index := old + 1; index <= rf.Size(); index++ {
		if rf.LogAt(index).Term != rf.CurrentTerm {
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
		Debug(dCommit, "s%d(leader) commit index from=%d to=%d match_index=%+v\n", rf.me, old+1, rf.commitIndex, rf.matchIndex)
		go rf.ApplyCommand()
	}
}

func (rf *Raft) ApplyCommand() {
	rf.mu.Lock()
	msgs := []ApplyMsg{}
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{
			CommandValid: true,
			CommandIndex: i,
			Command:      rf.LogAt(i).Command,
		}
		msgs = append(msgs, msg)
	}
	rf.lastApplied = rf.commitIndex
	rf.mu.Unlock()

	for _, m := range msgs {
		rf.applyCh <- m
	}
}

func (rf *Raft) ApplySnapshot(snapshot []byte, term int, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      snapshot,
		SnapshotTerm:  term,
		SnapshotIndex: index,
	}
}

func (rf *Raft) Majority() int {
	return len(rf.peers)/2 + 1
}

func (rf *Raft) AskVote(ctx context.Context, wg *sync.WaitGroup, server int, candidate int, term int, out chan int) {
	defer wg.Done()
	lastLogIndex := -1
	lastLogTerm := -1
	if len(rf.Logs) == 1 {
		lastLogIndex = rf.LastIncludedIndex
		lastLogTerm = rf.LastIncludedTerm
	} else {
		lastLogIndex = rf.Size()
		lastLogTerm = rf.LogAt(lastLogIndex).Term
	}
	args := &RequestVoteArgs{
		Term:         term,
		CandidateId:  candidate,
		LastLogTerm:  lastLogTerm,
		LastLogIndex: lastLogIndex,
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
	Debug(dVote, "RequestVote: s%d s%d args=%+v reply=%+v", rf.me, server, args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status != CANDIDATE || rf.CurrentTerm != term {
		var errMsg string
		if rf.status == FOLLOWER {
			errMsg = "follower"
		} else {
			errMsg = "candidate"
		}
		Debug(dVote, "s%d not a candidate anymore now %s ; cancel vote", rf.me, errMsg)
		out <- -1
	} else if reply.Term > rf.CurrentTerm {
		rf.CurrentTerm = reply.Term
		rf.status = FOLLOWER
		out <- -1
	} else if reply.VoteGranted {
		out <- 1
	} else {
		out <- 0
	}

}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.status == FOLLOWER && rf.timeout.Before(time.Now()) {
			rf.CurrentTerm += 1
			rf.VoteFor = rf.me
			rf.status = CANDIDATE
			rf.timeout = getRandomTimeout()
			timeout := rf.timeout
			Debug(dTimer, "s%d starts election with term %d", rf.me, rf.CurrentTerm)
			var wg sync.WaitGroup
			wg.Add(len(rf.peers) - 1)
			ctx, cancel := context.WithCancel(context.Background())
			out := make(chan int, len(rf.peers))
			candidateId := rf.me
			curTerm := rf.CurrentTerm
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
			for timeout.After(time.Now()) && run {
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
							rf.resetIndex()
							Debug(dLeader, "s%d wins election", rf.me)
							run = false
						} else if n-votes >= rf.Majority() {
							rf.status = FOLLOWER
							Debug(dLeader, "s%d lose election in term %d", rf.me, rf.CurrentTerm)
							run = false
						}
					}
					rf.mu.Unlock()
				default:
					// do nothing
					time.Sleep(10 * time.Millisecond)
				}
			}
			cancel()
			wg.Wait()
			close(out)
			rf.mu.Lock()
			if rf.status != LEADER {
				rf.status = FOLLOWER
				rf.VoteFor = NO_LEADER
				Debug(dVote, "s%d loses election in term %d\n", rf.me, rf.CurrentTerm)
			} else {
				Debug(dVote, "s%d wins election in term %d\n", rf.me, rf.CurrentTerm)
			}
			rf.persist()
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(10 * time.Millisecond)
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
	rf.VoteFor = NO_LEADER
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
	rf.Logs = make([]LogEntry, 1, MAX_LOGS)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if rf.persister.SnapshotSize() > 0 {
		rf.commitIndex = rf.LastIncludedIndex
		rf.lastApplied = rf.LastIncludedIndex
		go rf.ApplySnapshot(rf.persister.ReadSnapshot(), rf.LastIncludedTerm, rf.LastIncludedIndex)
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeats()

	return rf
}
