package mr

import "sync"

type SafeCounter struct {
	Count int
	Cond  *sync.Cond
}

func (sc *SafeCounter) Lock() {
	sc.Cond.L.Lock()
}

func (sc *SafeCounter) Wait() {
	sc.Cond.Wait()
}

func (sc *SafeCounter) Unlock() {
	sc.Cond.L.Unlock()
}

func (sc *SafeCounter) Broadcast() {
	sc.Cond.Broadcast()
}

func (sc *SafeCounter) Signal() {
	sc.Cond.Signal()
}
