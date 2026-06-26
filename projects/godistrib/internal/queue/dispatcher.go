package queue

import (
	"container/heap"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// Dispatcher is the brain of our scheduler
type Dispatcher struct {
	mu    sync.RWMutex
	Queue TaskQueue
}

// NewDispatcher initializes our protected heap
func NewDispatcher() *Dispatcher {
	pq := make(TaskQueue, 0)
	heap.Init(&pq) 
	return &Dispatcher{
		Queue: pq, 
	}
}

// HandleGetLen allows infinite concurrent reads safely
func (d *Dispatcher) HandleGetLen(w http.ResponseWriter, r *http.Request) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"queue_length": %d}`, d.Queue.Len())
}

// HandleSubmitTask securely decodes JSON and locks the heap to inject a new task
func (d *Dispatcher) HandleSubmitTask(w http.ResponseWriter, r *http.Request) {
	// FIX: Removed "queue." prefix because we are already inside package queue!
	var newTask Task
	if err := json.NewDecoder(r.Body).Decode(&newTask); err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Populate the timestamp for our FIFO tie-breaker
	newTask.Timestamp = time.Now().UnixNano()

	d.mu.Lock()
	defer d.mu.Unlock()

	heap.Push(&d.Queue, &newTask)
	
	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintf(w, `{"status": "task accepted", "id": "%s"}`, newTask.ID)
}