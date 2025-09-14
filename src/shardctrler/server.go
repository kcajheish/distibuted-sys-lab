package shardctrler

import (
	"errors"
	"log"
	"slices"
	"sort"
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

type ShardCtrler struct {
	mu         sync.Mutex
	me         int
	rf         *raft.Raft
	applyCh    chan raft.ApplyMsg
	cmdToOp    map[int]Op // command index -> op
	replyCache map[int64]any
	wait       map[int64]WaitFor // client id -> waited command
	PersistentData
	persister *raft.Persister
	dead      int32
}

type PersistentData struct {
	LastReq          map[int64]int64 // client id -> request id
	Configs          []Config        // indexed by config num
	SnapshotIndex    int
	LastCommandIndex int
}

type WaitFor struct {
	index int
	reqID int64
	done  chan any
}

type Op struct {
	Service  string
	Method   Method
	ClientID int64
	ID       int64
	Servers  map[int][]string
	Shard    int
	Num      int
	GIDS     []int
	GID      int
}

type Method string

const JOIN Method = "Join"

const LEAVE Method = "Leave"

const MOVE Method = "Move"

const QUERY Method = "Query"

const UNASSIGNED int = -1

const SERVICE = "Shard Controller"

var NewTermError = errors.New("new term")

var CmdOverrideByNewLeaderError = errors.New("command override by new leader")

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	sc.mu.Lock()
	if res, ok := sc.replyCache[args.ReqID]; ok {
		if r, ok := res.(JoinReply); ok {
			*reply = r
			sc.mu.Unlock()
			return
		}
	}

	op := Op{
		Service:  SERVICE,
		Method:   JOIN,
		ClientID: args.ClientID,
		ID:       args.ReqID,
		Servers:  args.Servers,
	}
	cmdIndex, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	sc.cmdToOp[cmdIndex] = op
	wait := WaitFor{
		index: cmdIndex,
		reqID: args.ReqID,
		done:  make(chan any),
	}
	sc.wait[args.ClientID] = wait
	sc.mu.Unlock()

	res := <-wait.done

	sc.mu.Lock()
	if err, ok := res.(error); ok {
		if err == NewTermError || err == CmdOverrideByNewLeaderError {
			reply.WrongLeader = true
			reply.Err = Err(err.Error())
		} else {
			log.Panicf("unrecognized errors: %+v", err)
		}
		sc.mu.Unlock()
		return
	}

	sc.replyCache[args.ReqID] = *reply
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	sc.mu.Lock()
	if res, ok := sc.replyCache[args.ReqID]; ok {
		if r, ok := res.(LeaveReply); ok {
			*reply = r
			sc.mu.Unlock()
			return
		}
	}

	op := Op{
		Service:  SERVICE,
		Method:   LEAVE,
		ClientID: args.ClientID,
		ID:       args.ReqID,
		GIDS:     args.GIDs,
	}
	cmdIndex, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	sc.cmdToOp[cmdIndex] = op
	wait := WaitFor{
		index: cmdIndex,
		reqID: args.ReqID,
		done:  make(chan any),
	}
	sc.wait[args.ClientID] = wait
	sc.mu.Unlock()

	res := <-wait.done

	sc.mu.Lock()
	if err, ok := res.(error); ok {
		if err == NewTermError || err == CmdOverrideByNewLeaderError {
			reply.WrongLeader = true
			reply.Err = Err(err.Error())
		} else {
			log.Panicf("unrecognized errors: %+v", err)
		}
		sc.mu.Unlock()
		return
	}

	sc.replyCache[args.ReqID] = *reply
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	sc.mu.Lock()
	if res, ok := sc.replyCache[args.ReqID]; ok {
		if r, ok := res.(MoveReply); ok {
			*reply = r
			sc.mu.Unlock()
			return
		}
	}

	op := Op{
		Service:  SERVICE,
		Method:   MOVE,
		ClientID: args.ClientID,
		ID:       args.ReqID,
		Shard:    args.Shard,
		GID:      args.GID,
	}
	cmdIndex, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	sc.cmdToOp[cmdIndex] = op
	wait := WaitFor{
		index: cmdIndex,
		reqID: args.ReqID,
		done:  make(chan any),
	}
	sc.wait[args.ClientID] = wait
	sc.mu.Unlock()

	res := <-wait.done

	sc.mu.Lock()
	if err, ok := res.(error); ok {
		if err == NewTermError || err == CmdOverrideByNewLeaderError {
			reply.WrongLeader = true
			reply.Err = Err(err.Error())
		} else {
			log.Panicf("unrecognized errors: %+v", err)
		}
		sc.mu.Unlock()
		return
	}

	sc.replyCache[args.ReqID] = *reply
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	sc.mu.Lock()
	if res, ok := sc.replyCache[args.ReqID]; ok {
		if r, ok := res.(QueryReply); ok {
			*reply = r
			sc.mu.Unlock()
			return
		}
	}

	op := Op{
		Service:  SERVICE,
		Method:   QUERY,
		ClientID: args.ClientID,
		ID:       args.ReqID,
		Num:      args.Num,
	}
	cmdIndex, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	sc.cmdToOp[cmdIndex] = op
	wait := WaitFor{
		index: cmdIndex,
		reqID: args.ReqID,
		done:  make(chan any),
	}
	sc.wait[args.ClientID] = wait
	sc.mu.Unlock()

	res := <-wait.done

	sc.mu.Lock()
	if err, ok := res.(error); ok {
		if err == NewTermError || err == CmdOverrideByNewLeaderError {
			reply.WrongLeader = true
			reply.Err = Err(err.Error())
		} else {
			log.Panicf("unrecognized errors: %+v", err)
		}
		sc.mu.Unlock()
		return
	}

	if conf, ok := res.(Config); ok {
		reply.Config = conf
	} else {
		log.Panicf("unexpected result from apply layer")
	}

	sc.replyCache[args.ReqID] = *reply
	sc.mu.Unlock()
}

type entry struct {
	gid   int
	count int
}

// rebalance shard among available gids.
func (sc *ShardCtrler) rebalance(config *Config) {
	ngroup := len(config.Groups)
	if ngroup == 0 {
		for s := 0; s < NShards; s++ {
			sc.assign(s, UNASSIGNED, config)
		}
		return
	}
	target := NShards / ngroup
	rem := NShards % ngroup

	entries := make([]entry, 0)
	for gid, c := range config.Load {
		e := entry{
			gid:   gid,
			count: c,
		}
		entries = append(entries, e)
	}

	sort.Slice(entries, func(i, j int) bool {

		return entries[i].count < entries[j].count
	})

	l, r := 0, ngroup-1
	bound := ngroup - rem
	for i := 0; i < NShards; i++ {
		if config.Shards[i] == UNASSIGNED {
			t := target + l/bound
			if entries[l].count >= t {
				log.Panicf("gid should have less shard target=%d, entries=%+v", t, entries)
			}

			sc.assign(i, entries[l].gid, config)
			entries[l].count += 1

			if entries[l].count == t {
				l += 1
			}
		}
	}

	for l < r {
		ltarget := target + l/bound
		rtarget := target + r/bound
		if entries[l].count == ltarget {
			l += 1
		} else if entries[r].count == rtarget {
			r -= 1
		} else {
			rgid := entries[r].gid
			lgid := entries[l].gid
			index := slices.Index(config.Shards[:], rgid)
			if index == -1 {
				log.Panicf("can't find gid=%d in shards=%+v", rgid, config.Shards)
			}
			sc.assign(index, lgid, config)
			entries[r].count -= 1
			entries[l].count += 1
		}
	}

}

func (sc *ShardCtrler) assign(shard int, gid int, config *Config) {
	if config.Shards[shard] == UNASSIGNED && gid == UNASSIGNED {
		return
	}

	if gid == UNASSIGNED {
		prevGID := config.Shards[shard]
		config.Shards[shard] = UNASSIGNED
		config.Load[prevGID] -= 1
		return
	}

	if _, ok := config.Groups[gid]; !ok {
		log.Panicf("can't assign to the gid not in the group: shard=%d, gid=%d, config=%+v", shard, gid, config)
	}

	if config.Shards[shard] == UNASSIGNED {
		config.Shards[shard] = gid
		config.Load[gid] += 1
		return
	}

	prevGID := config.Shards[shard]
	if prevGID == gid {
		DPrintf("already assign shard %d to the group %d; config=%+v", shard, gid, config)
		return
	}
	config.Load[prevGID] -= 1
	config.Shards[shard] = gid
	config.Load[gid] += 1

}

// unassigned the shard for a gid
// remove the gid from the group
func (sc *ShardCtrler) removeGroup(gid int, config *Config) {
	if config.Num != -1 {
		log.Panicf("can't unassigned in existing configurations")
	}

	for i := 0; i < NShards; i++ {
		if config.Shards[i] == gid {
			sc.assign(i, UNASSIGNED, config)
		}
	}
	delete(config.Groups, gid)
	delete(config.Load, gid)
}

func (sc *ShardCtrler) AddGroup(gid int, servers []string, config *Config) {
	if _, ok := config.Groups[gid]; ok {
		log.Panicf("can't add a group that already existed")
	}
	config.Groups[gid] = servers
	config.Load[gid] = 0
}

func (sc *ShardCtrler) copy(config *Config) Config {
	copyConfig := Config{}
	copyConfig.Num = -1
	copyConfig.Shards = [NShards]int{}
	for i := 0; i < NShards; i++ {
		copyConfig.Shards[i] = config.Shards[i]
	}
	copyConfig.Groups = make(map[int][]string, len(config.Groups))
	copyConfig.Load = make(map[int]int, len(config.Load))
	for k, v := range config.Groups {
		copyConfig.Groups[k] = v
	}
	for k, v := range config.Load {
		copyConfig.Load[k] = v
	}
	return copyConfig
}

func (sc *ShardCtrler) addConfig(config Config) {
	if config.Num != -1 {
		log.Panicf("can't assign the config twice.")
	}
	config.Num = len(sc.Configs)
	sc.Configs = append(sc.Configs, config)
}

func (sc *ShardCtrler) lastConfig() Config {
	n := len(sc.Configs)
	return sc.Configs[n-1]
}

func (sc *ShardCtrler) killed() bool {
	return atomic.LoadInt32(&sc.dead) == 1
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// apply messages to the state machine
func (sc *ShardCtrler) apply(op *Op) any {
	var res any
	switch op.Method {
	case JOIN:
		size := len(sc.Configs)
		newConfig := sc.copy(&sc.Configs[size-1])
		for gid, servers := range op.Servers {
			sc.AddGroup(gid, servers, &newConfig)
		}
		sc.rebalance(&newConfig)
		sc.addConfig(newConfig)
		DPrintf("JOIN new_config: %+v", sc.lastConfig())
		res = struct{}{}
	case LEAVE:
		size := len(sc.Configs)
		newConfig := sc.copy(&sc.Configs[size-1])
		for _, gid := range op.GIDS {
			sc.removeGroup(gid, &newConfig)
		}
		sc.rebalance(&newConfig)
		sc.addConfig(newConfig)
		DPrintf("LEAVE new_config=%+v", sc.lastConfig())
		res = struct{}{}
	case MOVE:
		size := len(sc.Configs)
		newConfig := sc.copy(&sc.Configs[size-1])
		sc.assign(op.Shard, op.GID, &newConfig)
		sc.addConfig(newConfig)
		res = struct{}{}
	case QUERY:
		var n int
		if op.Num >= len(sc.Configs) || op.Num < 0 {
			n = len(sc.Configs) - 1
		} else {
			n = op.Num
		}
		res = sc.copy(&sc.Configs[n])
	default:
		log.Panicf("unexpected method: %+v", op.Method)
	}
	return res
}

// Retrieve apply messages from apply channel and process it.
func (sc *ShardCtrler) process() {
	DPrintf("start processing")
	for msg := range sc.applyCh {
		if msg.CommandValid {
			sc.mu.Lock()
			if sc.LastCommandIndex+1 != msg.CommandIndex {
				DPrintf("ignore out of order messages: last_cmd_index=%d command_index=%d", sc.LastCommandIndex, msg.CommandIndex)
				sc.mu.Unlock()
				continue
			}
			sc.LastCommandIndex = max(sc.LastCommandIndex, msg.CommandIndex)
			op := msg.Command.(Op)
			DPrintf("s%d process apply_msg=%+v", sc.me, msg)

			if waitOp, ok := sc.cmdToOp[msg.CommandIndex]; ok && waitOp.ID != op.ID {
				if wait, ok := sc.wait[op.ClientID]; ok && waitOp.ID == wait.reqID {
					delete(sc.wait, op.ClientID)
					wait.done <- CmdOverrideByNewLeaderError
					close(wait.done)
				}
			}

			if reqID, ok := sc.LastReq[op.ClientID]; ok && reqID >= op.ID {
				if op.Method != QUERY {
					if wait, ok := sc.wait[op.ClientID]; ok && wait.reqID == op.ID {
						delete(sc.wait, op.ClientID)
						wait.done <- struct{}{}

						// Only the producer of the channel knows when the stream ends.
						// Therefore, close the done channel at producer-side
						close(wait.done)
					}
					sc.mu.Unlock()
					continue
				}

			}

			sc.LastReq[op.ClientID] = max(op.ID, sc.LastReq[op.ClientID])

			res := sc.apply(&op)

			if wait, ok := sc.wait[op.ClientID]; ok && wait.reqID == op.ID {
				delete(sc.wait, op.ClientID)
				wait.done <- res
				close(wait.done)
			}

			sc.mu.Unlock()
		}
	}
}

func (sc *ShardCtrler) watchTerm(interval time.Duration) {
	term, _ := sc.rf.GetState()
	for !sc.killed() {
		time.Sleep(interval)
		sc.mu.Lock()
		nextTerm, _ := sc.rf.GetState()
		if nextTerm != term {
			for clientID, wait := range sc.wait {
				delete(sc.wait, clientID)
				wait.done <- NewTermError
				close(wait.done)
			}
		}
		term = nextTerm
		sc.mu.Unlock()
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.Configs = make([]Config, 1)
	sc.Configs[0].Groups = make(map[int][]string)
	sc.Configs[0].Load = make(map[int]int)
	sc.Configs[0].Shards = [NShards]int{}
	sc.Configs[0].Num = 0
	for s := 0; s < NShards; s++ {
		sc.Configs[0].Shards[s] = UNASSIGNED
	}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.persister = persister

	// init data
	sc.LastReq = make(map[int64]int64)
	sc.cmdToOp = make(map[int]Op)
	sc.SnapshotIndex = 0
	sc.LastCommandIndex = 0
	sc.wait = make(map[int64]WaitFor)
	sc.replyCache = make(map[int64]any)

	// receive and process stream of apply messages from raft
	go sc.process()

	// watch out for change of term
	go sc.watchTerm(time.Duration(300 * time.Millisecond))

	return sc
}
