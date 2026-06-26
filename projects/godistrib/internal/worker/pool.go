package worker

import (
	"fmt"
	"sync"

	"github.com/anshgaur1396-svg/godistrib/internal/queue"
)

// Pool manages our concurrent workers
type Pool struct {
	TaskChan   chan *queue.Task
	NumWorkers int
	WG         *sync.WaitGroup 
}

// NewPool initializes the pool architecture.
func NewPool(numWorkers int, bufferSize int) *Pool {
	return &Pool{
		TaskChan:   make(chan *queue.Task, bufferSize),
		NumWorkers: numWorkers,
		WG:         &sync.WaitGroup{},
	}
}

// Start launches the worker goroutines.
func (p *Pool) Start() {
	for i := 1; i <= p.NumWorkers; i++ {
		p.WG.Add(1) // Register a new worker with the WaitGroup
		
		// Spawn the concurrent worker
		go func(workerID int) {
			defer p.WG.Done() // Signal the WaitGroup when this goroutine exits
			
			// The infinite listening loop
			for task := range p.TaskChan {
				// Simulating the heavy lifting
				fmt.Printf("Worker %d processing Task %s (Priority: %d)\n", workerID, task.ID, task.Priority)
			}
			
			fmt.Printf("Worker %d shutting down gracefully.\n", workerID)
		}(i)
	}
}