package kvraft

import (
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

	store   map[string][]byte
	wait    map[int64]WaitFor
	cmdToOp map[int]Op

	lastReqID map[int64]int64
	lastReply map[int64]any
	lastApply map[int64]any
}

// A previous command can cancel client's wait earilier since the command arrives late.
// Therefore, be explicit about what request ID the client wait for.
type WaitFor struct {
	CommandIndex int
	RequestID    int64
	Done         chan any
}

func (kv *KVServer) InitMap() {
	kv.store = make(map[string][]byte)
	kv.wait = make(map[int64]WaitFor)
	kv.cmdToOp = make(map[int]Op)
	kv.lastReqID = make(map[int64]int64)
	kv.lastReply = make(map[int64]any)
	kv.lastApply = make(map[int64]any)

}

func (kv *KVServer) cache(clientID int64, reply any) {
	kv.lastReply[clientID] = reply

}

func (kv *KVServer) cacheApplyResult(clientID int64, res any) {
	kv.lastApply[clientID] = res
}

func (kv *KVServer) getApplyResultFromCache(cliendID int64) any {
	if res, ok := kv.lastApply[cliendID]; ok {
		return res
	}
	return nil
}

func (kv *KVServer) getFromCache(clientID int64) any {
	if reply, ok := kv.lastReply[clientID]; ok {
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

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	if prevReply, ok := kv.getFromCache(args.ClientID).(*GetReply); ok && prevReply != nil && prevReply.ID == args.ID {
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

	reply.ID = args.ClientID
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

		kv.mu.Lock()
		kv.cache(args.ClientID, reply)
		kv.mu.Unlock()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	if prevReply, ok := kv.getFromCache(args.ClientID).(*PutAppendReply); ok && prevReply != nil && prevReply.ID == args.ID {
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

		kv.mu.Lock()
		kv.cache(args.ClientID, reply)
		kv.mu.Unlock()
	}
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
			op := msg.Command.(Op)

			DPrintf("s%d process apply_msg=%+v", kv.me, msg)

			kv.mu.Lock()
			if reqID, ok := kv.lastReqID[op.ClientID]; ok && reqID >= op.ID {
				// duplicate commands
				if wait, ok := kv.wait[op.ClientID]; ok && wait.RequestID == op.ID {
					delete(kv.wait, op.ClientID)
					res := kv.getApplyResultFromCache(op.ClientID)
					wait.Done <- res
					// Only the producer of the channel knows when the stream ends.
					// Therefore, close the done channel at producer-side
					close(wait.Done)
				}
				kv.mu.Unlock()
				continue
			}

			// Detect election because index is taken by a different command.
			if waitOp, ok := kv.cmdToOp[msg.CommandIndex]; ok && waitOp.ID != op.ID {
				if wait, ok := kv.wait[op.ClientID]; ok && wait.RequestID == op.ID {
					// how to tell client about election and ask client to retry?
					delete(kv.wait, op.ClientID)
					wait.Done <- CmdOverrideByNewLeaderError
					close(wait.Done)
				}
				kv.mu.Unlock()
				continue
			}

			kv.lastReqID[op.ClientID] = op.ID
			var res any
			switch op.Name {
			case GET:
				if val, ok := kv.store[op.Key]; ok {
					res = string(val)
				} else {
					res = KeyNotFoundErr
				}
			case PUT:
				kv.store[op.Key] = []byte(op.Value)
				res = struct{}{}
			case APPEND:
				if _, ok := kv.store[op.Key]; !ok {
					kv.store[op.Key] = []byte(op.Value)
				} else {
					kv.store[op.Key] = append(kv.store[op.Key], []byte(op.Value)...)
				}
				res = struct{}{}
			default:
				panic("unknow operation")

			}

			kv.cacheApplyResult(op.ClientID, res)

			if wait, ok := kv.wait[op.ClientID]; ok && wait.RequestID == op.ID {
				delete(kv.wait, op.ClientID)
				wait.Done <- res
				close(wait.Done)
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

	go kv.process()

	// When a server changes term, a new leader wins election.
	// Ask the client to retry with the new leader.
	// How often does it have to check term? Duration is at most election timeout.
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

	return kv
}
