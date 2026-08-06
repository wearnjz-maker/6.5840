package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu   sync.Mutex
	Data map[string]struct {
		Value   string
		Version rpc.Tversion
	}
	// Your definitions here.
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key := args.Key
	item, exists := kv.Data[key]
	if !exists {
		reply.Err = rpc.ErrNoKey
	} else {
		reply.Err = rpc.OK
		reply.Value = item.Value
		reply.Version = item.Version
	}

}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key := args.Key
	item, exists := kv.Data[key]
	if !exists {
		if args.Version != 0 {
			reply.Err = rpc.ErrNoKey
		} else {
			kv.Data[key] = struct {
				Value   string
				Version rpc.Tversion
			}{
				Value:   args.Value,
				Version: args.Version + 1,
			}
			reply.Err = rpc.OK
		}
	} else {
		if args.Version != item.Version {
			reply.Err = rpc.ErrVersion
		} else {
			kv.Data[key] = struct {
				Value   string
				Version rpc.Tversion
			}{
				Value:   args.Value,
				Version: args.Version + 1,
			}
			reply.Err = rpc.OK
		}
	}
	// Your code here.
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	kv.Data = make(map[string]struct {
		Value   string
		Version rpc.Tversion
	})
	return []any{kv}
}
