package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	lastLeader int
	clientID   int64
	n          int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (ck *Clerk) nextID() int64 {
	return atomic.AddInt64(&ck.n, 1)
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.lastLeader = 0
	ck.clientID = nrand()
	ck.n = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{
		ClientID: ck.clientID,
		ReqID:    ck.nextID(),
		Num:      num,
	}
	for {
		// try each known server.
		for i := 0; i < len(ck.servers); i++ {
			srv := ck.servers[(i+ck.lastLeader)%len(ck.servers)]
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.lastLeader = i
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{
		ClientID: ck.clientID,
		ReqID:    ck.nextID(),
		Servers:  servers,
	}

	for {
		// try each known server.
		for i := 0; i < len(ck.servers); i++ {
			srv := ck.servers[(ck.lastLeader+i)%len(ck.servers)]
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.lastLeader = i
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{
		ClientID: ck.clientID,
		ReqID:    ck.nextID(),
		GIDs:     gids,
	}

	for {
		// try each known server.
		for i := 0; i < len(ck.servers); i++ {
			srv := ck.servers[(ck.lastLeader+i)%len(ck.servers)]
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.lastLeader = i
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{
		ClientID: ck.clientID,
		ReqID:    ck.nextID(),
		Shard:    shard,
		GID:      gid,
	}

	for {
		// try each known server.
		for i := 0; i < len(ck.servers); i++ {
			srv := ck.servers[(ck.lastLeader+i)%len(ck.servers)]
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.lastLeader = i
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
