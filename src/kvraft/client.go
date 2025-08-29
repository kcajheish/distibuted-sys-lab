package kvraft

import (
	"crypto/rand"
	"log"
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
	count      atomic.Int64
}

func (ck *Clerk) NextReqID() int64 {
	return ck.count.Add(1)
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
			ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
			if ok && reply.Err == OK {
				ck.lastLeader = server
				DPrintf("client to s%d args=%+v reply=%+v", i, args, reply)
				return reply.Value
			}

			if ok && reply.Err == ErrNoKey {
				ck.lastLeader = server
				return ""
			}

			if !ok || ok && (reply.Err == ErrWrongLeader) {
				// try the next kv server
				continue
			}

			log.Panicf("unknow reply state; args=%+v reply=%+v", args, reply)
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

			ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
			if ok && reply.Err == OK {
				ck.lastLeader = server
				DPrintf("client to s%d args=%+v reply=%+v", i, args, reply)
				return
			}

			if !ok || ok && (reply.Err == ErrWrongLeader) {
				continue
			}

			log.Panicf("unknow reply state; args=%+v reply=%+v", args, reply)
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
