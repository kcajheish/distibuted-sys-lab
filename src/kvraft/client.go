package kvraft

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	ID         int64
	lastLeader int
}

var counter atomic.Int64

const MAX_ID int64 = (1 << 63) - 1 // 9223372036854775807

func (ck *Clerk) NextReqID() int64 {
	// expected:
	// sign bit   0              1              0 ...
	//  value     0 -> MAX_ID -> 0 -> MAX_ID -> 0 ...
	return counter.Add(1) & MAX_ID
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.ID = nrand()
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key
	args.ID = ck.NextReqID()
	args.ClientID = ck.ID

	for {
		for i := 0; i < len(ck.servers); i++ {
			server := (ck.lastLeader + i) % len(ck.servers)
			var reply GetReply
			msg := fmt.Sprintf("ck.Get: client=%d s%d request=%d args=%+v; ", ck.ID, server, args.ID, args)
			ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
			if ok && reply.Err == OK {
				ck.lastLeader = server
				msg += fmt.Sprintf("reply=%+v; ", reply)
				DPrintf(msg)
				return reply.Value
			}

			if ok && reply.Err == ErrNoKey {
				msg += ErrNoKey
				DPrintf(msg)
				ck.lastLeader = server
				return ""
			}

			if ok && (reply.Err == ErrWrongLeader) {
				// try the next kv server
				msg += ErrWrongLeader
				DPrintf(msg)
				continue
			}

			if !ok {
				msg += "rpc not receive reply from server"
				DPrintf(msg)
				continue
			}

		}
		time.Sleep(100 * time.Millisecond)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Op = op
	args.Key = key
	args.Value = value
	args.ID = ck.NextReqID()
	args.ClientID = ck.ID

	for {
		for i := 0; i < len(ck.servers); i++ {
			server := (ck.lastLeader + i) % len(ck.servers)
			var reply PutAppendReply
			msg := fmt.Sprintf("ck.PutAppend: client=%d s%d request=%d args=%+v; ", ck.ID, server, args.ID, args)
			ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
			if ok && reply.Err == OK {
				ck.lastLeader = server
				msg += fmt.Sprintf("reply=%+v; ", reply)
				DPrintf(msg)
				return
			}

			if ok && (reply.Err == ErrWrongLeader) {
				msg += ErrWrongLeader
				DPrintf(msg)
				continue
			}

			if !ok {
				msg += "rpc not receive reply from server"
				DPrintf(msg)
				continue
			}

		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
