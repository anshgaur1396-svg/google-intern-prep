package queue

// Task represents a unit of work to be processed.
type Task struct {
	ID       string
	Priority int    // Higher number = higher priority
	Payload  string // The actual data the worker needs
	Timestamp int64
	Index    int    // The index of the item in the heap
}

// TaskQueue implements heap.Interface and holds pointers to Tasks.
type TaskQueue []*Task

func (pq TaskQueue) Len() int { return len(pq) }

func (pq TaskQueue) Less(i, j int) bool {
	// Max-Heap: highest priority pops first
	if pq[i].Priority == pq[j].Priority {
		return pq[i].Timestamp < pq[j].Timestamp
	}
	return pq[i].Priority > pq[j].Priority
}

func (pq TaskQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

func (pq *TaskQueue) Push(x any) {
	n := len(*pq)
	task := x.(*Task)
	task.Index = n
	*pq = append(*pq, task)
}

func (pq *TaskQueue) Pop() any {
	old := *pq
	n := len(old)
	task := old[n-1]
	old[n-1] = nil  // Prevent memory leak! 
	task.Index = -1 // For safety
	*pq = old[0 : n-1]
	return task
}