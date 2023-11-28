package mr

import (
	"container/heap"
	"sync"
)

type SafeHeap struct {
	h  *PriorityQueue
	mu sync.RWMutex
}

func NewPQ(pq *PriorityQueue) SafeHeap {
	return SafeHeap{
		h: pq,
	}
}

func (sh *SafeHeap) Push(task *Task) {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	heap.Push(sh.h, task)
}

func (sh *SafeHeap) Pop() *Task {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if sh.h.Len() > 0 {
		return heap.Pop(sh.h).(*Task)
	}
	return nil
}

// update modifies the priority and value of an Item in the queue.
func (sh *SafeHeap) Done(task *Task) {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	heap.Remove(sh.h, task.index)
}

func (sh *SafeHeap) Top() *Task {
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	return sh.h.Top()
}

func (sh SafeHeap) Len() int {
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	return sh.h.Len()
}

func (sh *SafeHeap) ExpireAndPop(current, max int64) *Task {
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if top := sh.h.Top(); top != nil && current-top.UnixTime > max {
		return heap.Pop(sh.h).(*Task)
	}
	return nil
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue []*Task

func (pq PriorityQueue) Len() int {
	return len(pq)
}

func (pq PriorityQueue) Less(i, j int) bool {
	t := pq
	if t[i].Status == COMPLETED {
		return true
	}

	if t[j].Status == COMPLETED {
		return false
	}

	if t[i].JobType == t[j].JobType {
		return t[i].UnixTime <= t[j].UnixTime
	}

	if t[i].JobType == MAP {
		return true
	}

	if t[j].JobType == MAP {
		return false
	}

	return false
}

func (pq *PriorityQueue) Swap(i, j int) {
	t := *pq
	t[i], t[j] = t[j], t[i]
	t[i].index = i
	t[j].index = j
}

func (pq *PriorityQueue) Push(x any) {
	task := x.(*Task)
	n := len(*pq)
	task.index = n
	*pq = append(*pq, task)
}

func (pq *PriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1
	old[n-1] = nil
	*pq = old[:n-1]
	return item
}

func (pq *PriorityQueue) Top() *Task {
	n := len(*pq)
	if n == 0 {
		return nil
	}
	top := (*pq)[n-1]
	return top
}
