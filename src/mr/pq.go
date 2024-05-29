package mr

import (
	"container/heap"
	"sync"
)

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue struct {
	tasks []*Task
	mu    sync.RWMutex
}

func (pq *PriorityQueue) Len() int {
	pq.mu.RLock()
	defer pq.mu.RUnlock()
	return len(pq.tasks)
}

func (pq *PriorityQueue) Less(i, j int) bool {
	pq.mu.RLock()
	defer pq.mu.RUnlock()
	t := pq.tasks
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
	pq.mu.Lock()
	defer pq.mu.Unlock()
	t := pq.tasks
	t[i], t[j] = t[j], t[i]
	t[i].index = i
	t[j].index = j
}

func (pq *PriorityQueue) Push(x any) {
	task := x.(*Task)
	pq.mu.Lock()
	defer pq.mu.Unlock()
	n := len(pq.tasks)
	task.index = n
	pq.tasks = append(pq.tasks, task)
}

func (pq *PriorityQueue) Pop() any {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	old := pq.tasks
	n := len(old)
	item := old[n-1]
	item.index = -1
	old[n-1] = nil
	pq.tasks = old[:n-1]
	return item
}

// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) Done(task *Task) {
	task.Status = COMPLETED
	pq.mu.Lock()
	defer pq.mu.Unlock()
	heap.Fix(pq, task.index)
	heap.Pop(pq)
}

func (pq *PriorityQueue) Top() *Task {
	pq.mu.RLock()
	defer pq.mu.RUnlock()
	n := len(pq.tasks)
	if n == 0 {
		return nil
	}
	top := pq.tasks[n-1]
	return top
}
