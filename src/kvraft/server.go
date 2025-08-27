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

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Name  string // name of the operation e.g. Put, Get...etc
	Key   string
	Value string
	ID    uint64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	counter    atomic.Uint64
	next       chan interface{}
	store      map[string]string
	done       map[uint64]chan bool
	apply      map[uint64]raft.ApplyMsg
	reqToOpID  map[uint64]uint64
	indexToOp  map[int]uint64
	opToIndex  map[uint64]int
	reqToReply map[uint64]interface{}
	opIDToReq  map[uint64]uint64
}

func (kv *KVServer) NextID() uint64 {
	return kv.counter.Add(1)
}

func (kv *KVServer) ValidateOp(op Op, execOp Op) bool {
	return op.ID == execOp.ID
}

func (kv *KVServer) Clear(reqID uint64) {
	if _, ok := kv.reqToOpID[reqID]; !ok {
		return
	}
	opID := kv.reqToOpID[reqID]
	index := kv.opToIndex[opID]
	delete(kv.apply, opID)
	close(kv.done[opID])
	delete(kv.done, opID)
	delete(kv.reqToOpID, reqID)
	delete(kv.opToIndex, opID)
	delete(kv.indexToOp, index)
	delete(kv.reqToReply, reqID)
	delete(kv.opIDToReq, opID)
}

func (kv *KVServer) InitMap() {
	kv.store = make(map[string]string)
	kv.done = make(map[uint64]chan bool)
	kv.apply = make(map[uint64]raft.ApplyMsg)
	kv.reqToOpID = make(map[uint64]uint64)
	kv.indexToOp = make(map[int]uint64)
	kv.opToIndex = make(map[uint64]int)
	kv.reqToReply = make(map[uint64]interface{})
	kv.opIDToReq = make(map[uint64]uint64)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	if prevReply, ok := kv.reqToReply[args.ID]; ok { // duplicated request
		kv.mu.Unlock()
		*reply = prevReply.(GetReply)
		return
	}

	op := Op{}
	op.ID = kv.NextID()
	op.Key = args.Key
	op.Name = "Get"

	kv.done[op.ID] = make(chan bool)

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}

	if opID, ok := kv.indexToOp[index]; ok {
		kv.done[opID] <- false
		reqID := kv.opIDToReq[opID]
		kv.Clear(reqID)
	}

	kv.indexToOp[index] = op.ID
	kv.reqToOpID[args.LastReqId] = op.ID
	kv.opToIndex[op.ID] = index
	kv.opIDToReq[op.ID] = args.ID
	done := kv.done[op.ID]
	kv.mu.Unlock()

	status, ok := <-done
	defer func() {
		kv.next <- struct{}{}
	}()

	// Ask the client to try another server when either two happen:
	// Channel is already closed because the server is killed.
	// Command is not committed due to re-election;
	if !ok || !status {
		reply.Err = ErrNotCommitted
		return
	}

	msg := kv.apply[op.ID]
	if msg.CommandIndex != index {
		// out of order detected
		log.Panicf("ApplyMsg out of order apply_msg=%+v, expected_index=%d", msg, index)
	}

	execOp := msg.Command.(Op)
	if ok := kv.ValidateOp(op, execOp); !ok {
		log.Panicf("ApplyMsg out of order op=%+v, exec_op=%+v", op, execOp)
	}

	val, ok := kv.store[args.Key]
	if ok {
		reply.Value = val
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
	}

	kv.mu.Lock()
	kv.reqToReply[args.ID] = *reply

	if args.LastReqId != 0 {
		kv.Clear(args.LastReqId)
	}
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	if prevReply, ok := kv.reqToReply[args.ID]; ok {
		kv.mu.Unlock()
		// duplicate client request
		*reply = prevReply.(PutAppendReply)
		return
	}

	if args.LastReqId != 0 {
		kv.Clear(args.LastReqId)
	}

	op := Op{}
	op.Name = args.Op
	op.ID = kv.NextID()
	op.Key = args.Key
	op.Value = args.Value

	kv.done[op.ID] = make(chan bool)

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}

	if opID, ok := kv.indexToOp[index]; ok {
		kv.done[opID] <- false
		reqID := kv.opIDToReq[opID]
		kv.Clear(reqID)
	}

	kv.indexToOp[index] = op.ID
	done := kv.done[op.ID]
	kv.mu.Unlock()

	status, ok := <-done
	defer func() {
		kv.next <- struct{}{}
	}()

	if !ok || !status {
		reply.Err = ErrNotCommitted
		return
	}

	msg := kv.apply[op.ID]
	if msg.CommandIndex != index {
		// out of order detected
		log.Panicf("ApplyMsg out of order apply_msg=%+v, expected_index=%d", msg, index)
	}

	execOp := msg.Command.(Op)
	if ok := kv.ValidateOp(op, execOp); !ok {
		log.Panicf("ApplyMsg out of order op=%+v, exec_op=%+v", op, execOp)
	}

	if _, ok := kv.store[op.Key]; !ok {
		kv.store[args.Key] = ""
	}

	switch op.Name {
	case "Put":
		kv.store[args.Key] = args.Value
	case "Append":
		kv.store[args.Key] = kv.store[args.Key] + args.Value
	default:
		log.Fatalf("unexpected operations")
	}

	reply.Err = OK

	kv.mu.Lock()
	kv.reqToReply[args.ID] = *reply
	if args.LastReqId != 0 {
		kv.Clear(args.LastReqId)
	}
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
	close(kv.next)
	DPrintf("kv server %d killed.", kv.me)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
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

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.next = make(chan interface{})
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.InitMap()

	// You may need initialization code here.
	go func() {
		for msg := range kv.applyCh {
			if msg.CommandValid {
				op := msg.Command.(Op)
				kv.mu.Lock()
				kv.apply[op.ID] = msg
				done := kv.done[op.ID]
				kv.mu.Unlock()
				done <- true
				<-kv.next
			}
		}
		DPrintf("kv server %d stop receiving messages.", kv.me)

		for _, ch := range kv.done {
			close(ch)
		}
	}()

	return kv
}
