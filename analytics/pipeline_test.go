package analytics

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestPipeline_FanOutFanIn_And_Cancellation(t *testing.T) {
	numWorkers := 4
	workerBuf := 4

	p := NewPipeline(numWorkers, workerBuf)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Collect results here
	var collected []ProcessedEvent
	// Start Collector
	p.CollectResults(ctx, &collected)

	// Start workers (consume from worker chans)
	p.StartWorkers(ctx)

	// Start the producer goroutine
	p.StartProducer(ctx)

	// Run for a bit, then cancel
	time.Sleep(2 * time.Second)
	cancel()

	p.Wait()
	p.CloseChannels()

	// Allow results collector to finish
	time.Sleep(200 * time.Millisecond)

	dropped := p.DroppedSummary()
	totalDropped := 0
	for i, v := range dropped {
		totalDropped += v
		t.Logf("Worker %d dropped: %d", i, v)
	}

	t.Logf("Collected ProcessedEvents: %d", len(collected))
	t.Logf("Total Dropped: %d", totalDropped)

	// Race check: do not read/write past channels closure
	if len(collected)+totalDropped == 0 {
		t.Fatalf("Too few events processed/dropped")
	}
}

// TestDropCount verifies the producer skips full workers non-blockingly.
func TestPipeline_DropsWhenWorkersFull(t *testing.T) {
	numWorkers := 4
	workerBuf := 1 // tiny buffer to force drops

	p := NewPipeline(numWorkers, workerBuf)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Collect results
	var collected []ProcessedEvent
	p.CollectResults(ctx, &collected)
	p.StartWorkers(ctx)
	p.StartProducer(ctx)

	time.Sleep(2 * time.Second)
	cancel()
	p.Wait()
	p.CloseChannels()
	// Let collector finish
	time.Sleep(100 * time.Millisecond)
	dropped := p.DroppedSummary()
	totalDropped := 0
	for _, v := range dropped {
		totalDropped += v
	}
	if totalDropped == 0 {
		t.Fatalf("expected dropped events due to worker buffer full")
	}
}

// TestRaceLeak cancels quickly to check for leaks and races on shutdown.
func TestPipeline_RaceLeakAndOrderlyShutdown(t *testing.T) {
	numWorkers := 4
	workerBuf := 2

	p := NewPipeline(numWorkers, workerBuf)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	atomic.StoreInt32((*int32)(&p.DroppedCounts[0]), 0)

	// Collector
	var results []ProcessedEvent
	p.CollectResults(ctx, &results)
	p.StartWorkers(ctx)
	p.StartProducer(ctx)

	// Immediate cancel
	time.Sleep(30 * time.Millisecond)
	cancel()

	p.Wait()
	p.CloseChannels()
	time.Sleep(30 * time.Millisecond)
}