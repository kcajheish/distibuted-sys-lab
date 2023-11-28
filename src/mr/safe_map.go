package mr

import "sync"

type SafeMap struct {
	partitions map[int][]string
	mu         sync.RWMutex
}

func (sm *SafeMap) Read(key int) []string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.partitions[key]
}

func (sm *SafeMap) Append(key int, path string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.partitions[key] = append(sm.partitions[key], path)
}
