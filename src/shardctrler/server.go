package shardctrler

import (
	"errors"
	"log"
	"slices"
	"sort"
	"sync"
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
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	cmdToOp map[int]Op        // command index -> op
	wait    map[int64]WaitFor // client id -> waited command
	PersistentData
	persister *raft.Persister
}

type PersistentData struct {
	LastReq          map[int]int64 // client id -> request id
	Configs          []Config      // indexed by config num
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
	Method   string
	ClientID int64
	ID       int64
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
	// Your code here.
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
}

type entry struct {
	gid   int
	count int
}

// rebalance shard among available gids.
func (sc *ShardCtrler) rebalance(config *Config) {
	ngroup := len(config.Groups)
	target := NShards / ngroup
	rem := NShards % ngroup

	entries := make([]entry, ngroup)
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
	bound := ngroup - rem - 1
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
		log.Panicf("can't unassign a shard without gid")
	}

	if _, ok := config.Groups[gid]; !ok {
		log.Panicf("can't assign to the gid not in the group")
	}

	if config.Shards[shard] == UNASSIGNED {
		config.Shards[shard] = gid
		config.Load[gid] += 1
		return
	}

	if gid == UNASSIGNED {
		config.Shards[shard] = gid
		config.Load[gid] -= 1
		return
	}

	prevGid := config.Shards[shard]
	if prevGid == gid {
		DPrintf("already assign shard %d to the group %d; config=%+v", shard, gid, config)
		return
	}
	config.Load[prevGid] -= 1
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

func (sr *ShardCtrler) copy(config *Config) Config {
	copyConfig := Config{}
	copyConfig.Num = -1
	copyConfig.Shards = [NShards]int{}
	for i := 0; i < NShards; i++ {
		copyConfig.Shards[i] = config.Shards[i]
	}
	copyConfig.Groups = make(map[int][]string, len(config.Groups))
	for k, v := range config.Groups {
		copyConfig.Groups[k] = v
	}
	for k, v := range config.Load {
		copyConfig.Load[k] = v
	}
	return copyConfig
}

func (sr *ShardCtrler) addConfig(config Config) {
	if config.Num != -1 {
		log.Panicf("can't assign the config twice.")
	}
	config.Num = len(sr.Configs)
	sr.Configs = append(sr.Configs, config)
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// apply messages to the state machine
func (sr *ShardCtrler) apply(method Method, msg *raft.ApplyMsg) {
	switch method {
	case JOIN:
		//
	case LEAVE:
		//
	case MOVE:
		//
	case QUERY:
		//
	default:
		log.Panicf("unexpected method: %+v", method)
	}
}

// Retrieve apply messages from apply channel and process it.
func (sr *ShardCtrler) process() {}

func (sc *ShardCtrler) loadSnapshot() {}

func (sc *ShardCtrler) takeSnapshot(interval time.Duration) {}

func (sc *ShardCtrler) watchTerm(interval time.Duration) {}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.Configs = make([]Config, 1)
	sc.Configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.persister = persister

	// init data
	sc.LastReq = make(map[int]int64)
	sc.cmdToOp = make(map[int]Op)
	sc.SnapshotIndex = 0
	sc.LastCommandIndex = 0
	sc.wait = make(map[int64]WaitFor)

	// Install snapshot when the server restarts

	// receive and process stream of apply messages from raft

	// watch out for change of term

	// take snapshot periodically

	return sc
}
