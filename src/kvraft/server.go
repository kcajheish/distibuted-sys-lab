package kvraft

import (
	"bytes"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

var NewTermError = errors.New("new term")

var KeyNotFoundErr = errors.New("no key")

var DuplicateError = errors.New("duplicate error")

var CmdOverrideByNewLeaderError = errors.New("command override by new leader")

const GET = "Get"

const PUT = "Put"

const APPEND = "Append"

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Name     string // name of the operation e.g. Put, Get...etc
	Key      string
	Value    string
	ID       int64
	ClientID int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	Persistent
	wait      map[int64]WaitFor
	persister *raft.Persister
	lastReply map[int64]any
	cmdToOp   map[int]Op
}

type Persistent struct {
	Store             map[string][]byte
	LastReqID         map[int64]int64
	LastSnapshotIndex int
	LastCmdIndex      int
}

// A previous command can cancel client's wait earilier since the command arrives late.
// Therefore, be explicit about what request ID the client wait for.
type WaitFor struct {
	CommandIndex int
	RequestID    int64
	Done         chan any
}

func (kv *KVServer) InitMap() {
	kv.Store = make(map[string][]byte)
	kv.wait = make(map[int64]WaitFor)
	kv.cmdToOp = make(map[int]Op)
	kv.LastReqID = make(map[int64]int64)
	kv.lastReply = make(map[int64]any)

}

func (kv *KVServer) cache(reqID int64, reply any) {
	kv.lastReply[reqID] = reply

}

func (kv *KVServer) getFromCache(reqID int64) any {
	if reply, ok := kv.lastReply[reqID]; ok {
		return reply
	}
	return nil
}

func (kv *KVServer) setWait(clientID int64, reqID int64, cmdIndex int) WaitFor {
	kv.wait[clientID] = WaitFor{
		CommandIndex: cmdIndex,
		RequestID:    reqID,
		Done:         make(chan any),
	}
	return kv.wait[clientID]
}

func (kv *KVServer) clearCache(reqID int64) {
	delete(kv.lastReply, reqID)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	kv.clearCache(args.LastReqID)
	if prevReply, ok := kv.getFromCache(args.ID).(*GetReply); ok && prevReply != nil {
		kv.mu.Unlock()
		*reply = *prevReply
		return
	}

	op := Op{
		ID:       args.ID,
		Key:      args.Key,
		Name:     GET,
		ClientID: args.ClientID,
	}

	var res any
	index, _, isLeader := kv.rf.Start(op)
	DPrintf("KVServer.Get: s%d op=%+v cmd_index=%d", kv.me, op, index)
	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}
	kv.cmdToOp[index] = op
	wait := kv.setWait(args.ClientID, args.ID, index)
	kv.mu.Unlock()

	res, ok := <-wait.Done
	if !ok {
		panic("Get: can't close a channel that client waits for")
	}

	kv.mu.Lock()

	reply.ID = op.ID
	if err, ok := res.(error); ok {
		if errors.Is(err, KeyNotFoundErr) {
			reply.Value = ""
			reply.Err = ErrNoKey
		} else if errors.Is(err, CmdOverrideByNewLeaderError) {
			reply.Err = ErrWrongLeader
		} else if errors.Is(err, NewTermError) {
			reply.Err = ErrWrongLeader
		} else {
			panic("Get: unknow error ")
		}
	} else {
		reply.Value, ok = res.(string)
		reply.Err = OK
		if !ok {
			panic("Get:unexpected type; should be string value")
		}
		kv.cache(args.ID, reply)
	}
	delete(kv.cmdToOp, index)
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	kv.clearCache(args.LastReqID)
	if prevReply, ok := kv.getFromCache(args.ID).(*PutAppendReply); ok && prevReply != nil {
		kv.mu.Unlock()
		*reply = *prevReply
		return
	}

	op := Op{
		Name:     args.Op,
		ID:       args.ID,
		Key:      args.Key,
		Value:    args.Value,
		ClientID: args.ClientID,
	}

	var res any
	index, _, isLeader := kv.rf.Start(op)
	DPrintf("KVServer.PutAppend s%d op=%+v cmd_index=%d", kv.me, op, index)
	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}

	kv.cmdToOp[index] = op
	wait := kv.setWait(args.ClientID, args.ID, index)
	kv.mu.Unlock()

	processRes, ok := <-wait.Done
	if !ok {
		panic("PutAppend: can't close a channel that client waits for")
	}
	kv.mu.Lock()
	res = processRes
	reply.ID = op.ID
	if err, ok := res.(error); ok {
		if errors.Is(err, CmdOverrideByNewLeaderError) {
			reply.Err = ErrWrongLeader
		} else if errors.Is(err, NewTermError) {
			reply.Err = ErrWrongLeader
		}
	} else {
		reply.Err = OK
		kv.cache(args.ID, reply)
	}
	delete(kv.cmdToOp, index)
	kv.mu.Unlock()
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	time.Sleep(10 * time.Millisecond)
	DPrintf("kv server %d killed.", kv.me)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) process() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			kv.mu.Lock()
			if kv.LastCmdIndex+1 != msg.CommandIndex {
				DPrintf("ignore out of order messages: last_cmd_index=%d command_index=%d", kv.LastCmdIndex, msg.CommandIndex)
				kv.mu.Unlock()
				continue
			}
			kv.LastCmdIndex = max(kv.LastCmdIndex, msg.CommandIndex)
			op := msg.Command.(Op)
			DPrintf("s%d process apply_msg=%+v", kv.me, msg)

			// Detect election because index is taken by a different command.
			// After new election, commands are not committed and discarded because new leader doesn't have it.
			// Client will not receive apply messages for those discarded commands and wait forever.
			// Thus, we have to detect whether a command index has served a different client request.
			// If it does, inform waited client to retry the request.
			if waitOp, ok := kv.cmdToOp[msg.CommandIndex]; ok && waitOp.ID != op.ID {
				if wait, ok := kv.wait[op.ClientID]; ok && waitOp.ID == wait.RequestID {
					delete(kv.wait, op.ClientID)
					wait.Done <- CmdOverrideByNewLeaderError
					close(wait.Done)
				}
			}
			// Receive a duplicated command.
			// After old leader partitions, new leader wins the election. The client sends the request again to the new leader.
			// case a: old leader does not commit.
			// case b: old leader commits and applies the message before client sends the request.
			// case c: old leader commits and applies the message after client sends the request.
			// To deal with case b and c, we have to cache results so that
			// when the kv server receives duplicated command, it can still reply.
			if reqID, ok := kv.LastReqID[op.ClientID]; ok && reqID >= op.ID {
				if op.Name != GET {
					if wait, ok := kv.wait[op.ClientID]; ok && wait.RequestID == op.ID {
						delete(kv.wait, op.ClientID)
						wait.Done <- struct{}{}

						// Only the producer of the channel knows when the stream ends.
						// Therefore, close the done channel at producer-side
						close(wait.Done)
					}
					kv.mu.Unlock()
					continue
				}

			}

			kv.LastReqID[op.ClientID] = max(op.ID, kv.LastReqID[op.ClientID])

			var res any
			switch op.Name {
			case GET:
				if val, ok := kv.Store[op.Key]; ok {
					res = string(val)
				} else {
					res = KeyNotFoundErr
				}
			case PUT:
				kv.Store[op.Key] = []byte(op.Value)
				res = struct{}{}
			case APPEND:
				if _, ok := kv.Store[op.Key]; !ok {
					kv.Store[op.Key] = []byte(op.Value)
				} else {
					kv.Store[op.Key] = append(kv.Store[op.Key], []byte(op.Value)...)
				}
				res = struct{}{}
			default:
				panic("unknow operation")
			}

			if wait, ok := kv.wait[op.ClientID]; ok && wait.RequestID == op.ID {
				delete(kv.wait, op.ClientID)
				wait.Done <- res
				close(wait.Done)
			}
			kv.mu.Unlock()

		}

		if msg.SnapshotValid {
			kv.mu.Lock()
			buff := bytes.NewBuffer(msg.Snapshot)
			decode := labgob.NewDecoder(buff)
			var p Persistent
			if err := decode.Decode(&p); err != nil {
				DPrintf("s%d can't decode: snapshot_msg=%+v err=%+v", kv.me, msg, err)
			} else {
				kv.Persistent = p
			}
			kv.mu.Unlock()
		}
	}
	DPrintf("kv server %d stop receiving messages.", kv.me)
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.InitMap()

	kv.persister = persister

	// Without registering struct for any type in Persistent,
	// Encode returns "gob: type not registered for interface: struct {}"
	// It leads to unexpected EOF during decode
	// labgob.Register(KeyNotFoundErr)
	// labgob.Register(struct{}{})

	// restore from snapshot
	if persister.SnapshotSize() > 0 {
		buffer := bytes.NewBuffer(persister.ReadSnapshot())
		d := labgob.NewDecoder(buffer)
		var p Persistent
		if err := d.Decode(&p); err != nil {
			DPrintf("can't decode snapshot err=%+v", err)
		} else {
			kv.Persistent = p
		}
	}

	go kv.process()

	// When a server changes term, a new leader wins election.
	// Ask the client to retry with the new leader.
	// How often does it have to check term? Duration is at most election timeout.
	// When a partition server rejoins, current leader will start a new term
	// and the uncommitted command from previous term may never commit because raft can only commit command in current term.
	// Thus, we have to ask the client to retry.
	go func() {
		duration := 300 * time.Millisecond
		term, _ := kv.rf.GetState()
		for !kv.killed() {
			time.Sleep(duration)
			kv.mu.Lock()
			nextTerm, _ := kv.rf.GetState()
			if nextTerm != term {
				for clientID, wait := range kv.wait {
					delete(kv.wait, clientID)
					wait.Done <- NewTermError
					close(wait.Done)
				}
			}
			term = nextTerm
			kv.mu.Unlock()
		}
	}()

	go func() {
		if kv.maxraftstate == -1 {
			return
		}

		// How often to check?
		// Frequency is around the heartbeat interval.
		// If frequency is smaller, snapshot is up to date but cpu spends much time checking.
		// If frequency is larger, cpu spends time doing useful stuff but snapshot could be stale.
		duration := time.Duration(60 * time.Millisecond)
		for !kv.killed() {
			time.Sleep(duration)
			kv.mu.Lock()
			if kv.persister.RaftStateSize() >= kv.maxraftstate && kv.LastCmdIndex > kv.LastSnapshotIndex {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				if err := e.Encode(kv.Persistent); err != nil {
					DPrintf("%+v", err)
				}
				data := w.Bytes()
				kv.rf.Snapshot(kv.LastCmdIndex, data)
				kv.LastSnapshotIndex = kv.LastCmdIndex
			}
			kv.mu.Unlock()
		}
	}()

	return kv
}
