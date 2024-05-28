package mr

import (
	"sync"
)

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue struct {
	tasks []*Task
	mu    sync.RWMutex
}

func (pq *PriorityQueue) Len() int {}

func (pq *PriorityQueue) Less(i, j int) bool {}

func (pq *PriorityQueue) Swap(i, j int) {}

func (pq *PriorityQueue) Push(x any) {}

func (pq *PriorityQueue) Pop() any {}

// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) Done(task *Task) {}
