package main

import (
	"context"
	"container/heap" // <-- ADD THIS BACK
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/anshgaur1396-svg/godistrib/internal/queue"
	"github.com/anshgaur1396-svg/godistrib/internal/worker"
)

func main() {
	fmt.Println("Starting GoDistrib Scheduler...")

	// 1. Initialize Components
	dispatcher := queue.NewDispatcher()
	
	// Create a pool with 5 workers and a channel buffer of 10
	pool := worker.NewPool(5, 10) 
	pool.Start()

	// 2. The Link: Background goroutine feeding tasks from Heap to Workers
	go func() {
		for {
			dispatcher.Mu.Lock()
			if dispatcher.Queue.Len() > 0 {
				// Pop the highest priority task
				task := heap.Pop(&dispatcher.Queue).(*queue.Task)
				
				// Send it to the worker pool (safe because TaskChan is buffered)
				pool.TaskChan <- task 
			}
			dispatcher.Mu.Unlock()
			
			// Prevent aggressive CPU spinning if the queue is empty
			time.Sleep(100 * time.Millisecond) 
		}
	}()

	// 3. Register HTTP Routes
	http.HandleFunc("/length", dispatcher.HandleGetLen)
	http.HandleFunc("/submit", dispatcher.HandleSubmitTask)

	// 4. Initialize HTTP Server
	server := &http.Server{
		Addr:    ":8080",
		Handler: nil, 
	}

	// Run the server in a background goroutine so it doesn't block main
	go func() {
		fmt.Println("API listening on port 8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server crash: %v\n", err)
		}
	}()

	// 5. The Interceptor: Graceful Shutdown Logic
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	// The main thread BLOCKS here until you press Ctrl+C
	<-stopChan 
	fmt.Println("\nShutdown signal received. Commencing graceful teardown...")

	// Close the channel so workers finish their current tasks and exit
	close(pool.TaskChan)
	
	// Wait for all workers to report p.WG.Done()
	pool.WG.Wait() 
	fmt.Println("All workers safely spun down.")

	// Safely shut down the HTTP server
	if err := server.Shutdown(context.Background()); err != nil {
		log.Fatalf("HTTP server shutdown error: %v", err)
	}

	fmt.Println("GoDistrib offline. Zero data lost.")
}