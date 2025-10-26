package main

import (
	"analytics"
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	pipeline := analytics.NewPipeline(4, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Trap Ctrl+C
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Println("-- Received interrupt: shutting down...")
		cancel()
	}()

	results := make([]analytics.ProcessedEvent, 0, 256)
	pipeline.CollectResults(ctx, &results)
	pipeline.StartWorkers(ctx)
	pipeline.StartProducer(ctx)

	// Let the pipeline run for 5 seconds or until shutdown signal
	shutdownTimeout := time.After(5 * time.Second)
MainLoop:
	for {
		select {
		case <-shutdownTimeout:
			fmt.Println("-- Timeout reached. Shutting down...")
			break MainLoop
		case <-ctx.Done():
			break MainLoop
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}

	cancel()
	pipeline.Wait()
	pipeline.CloseChannels()

	fmt.Printf("\nEvents processed: %d\n", len(results))
	dropped := pipeline.DroppedSummary()
	for i, v := range dropped {
		fmt.Printf("Worker %d dropped: %d\n", i, v)
	}

	if len(results) > 0 {
		fmt.Println("-- First 5 processed events:")
		for i := 0; i < 5 && i < len(results); i++ {
			ev := results[i]
			fmt.Printf("Worker %d processed event %d at %s (value=%d)\n", ev.WorkerID, ev.Input.ID, ev.Input.Timestamp.Format(time.RFC3339Nano), ev.OutVal)
		}
	}
}