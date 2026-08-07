package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck   kvtest.IKVClerk
	name string
	// You may add code here
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck}
	lk.name = lockname
	lk.ck.Put(lockname, "", 0)
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	putString := kvtest.RandValue(8)
	for {
		value, version, _ := lk.ck.Get(lk.name)
		if value == "" {
			err := lk.ck.Put(
				lk.name,
				putString,
				version,
			)
			if err == rpc.OK {
				break
			}
			if err == rpc.ErrMaybe {
				value1, _, _ := lk.ck.Get(lk.name)
				if value1 == putString {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)

	}
}

func (lk *Lock) Release() {
	// Your code here
	name, version, err := lk.ck.Get(lk.name)
	if err == rpc.OK && name != "" {
		lk.ck.Put(lk.name, "", version)
	}

}
