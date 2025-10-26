package analytics

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// MetricEvent represents a single metric event.
type MetricEvent struct {
	ID        int
	Timestamp time.Time
	Payload   string
}

// ProcessedEvent represents a processed metric event by a worker.
type ProcessedEvent struct {
	WorkerID  int
	Input     MetricEvent
	OutVal    int  // Simulated out value; can be the ID * workerid, etc.
}

// Pipeline holds the core components and channels for analytics fan-out/fan-in.
type Pipeline struct {
	NumWorkers     int
	WorkerBufSize  int // Per-worker buffer size
	Ingest         chan MetricEvent
	WorkerChans    []chan MetricEvent
	ResultChan     chan ProcessedEvent
	DroppedCounts  []int // Number of dropped events per worker
	DropMu         sync.Mutex
	wg             sync.WaitGroup
}

func NewPipeline(numWorkers, workerBufSize int) *Pipeline {
	workerChans := make([]chan MetricEvent, numWorkers)
	for i := 0; i < numWorkers; i++ {
		workerChans[i] = make(chan MetricEvent, workerBufSize)
	}
	return &Pipeline{
		NumWorkers:    numWorkers,
		WorkerBufSize: workerBufSize,
		Ingest:        make(chan MetricEvent),
		WorkerChans:   workerChans,
		ResultChan:    make(chan ProcessedEvent, 128), // generously buffered collector
		DroppedCounts: make([]int, numWorkers),
	}
}

// Producer: emits MetricEvents (simulate with random sleeps), and fans out round-robin
// to the worker chans (non-blocking -- skip if worker buffered chan is full).
func (p *Pipeline) StartProducer(ctx context.Context) {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		var (
			eventID   = 0
			workerIdx = 0
		)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				// Simulate input interval
				t := time.Duration(50+rand.Intn(150)) * time.Millisecond
				time.Sleep(t)
			}

			// Simulate metric event
			evt := MetricEvent{
				ID:        eventID,
				Timestamp: time.Now(),
				Payload:   fmt.Sprintf("event-%d", eventID),
			}

			// Fan out round robin to workers non-blocking
			selected := workerIdx % p.NumWorkers
			select {
			case p.WorkerChans[selected] <- evt:
				// sent OK
			default:
				// buffer full; drop
				p.DropMu.Lock()
				p.DroppedCounts[selected]++
				p.DropMu.Unlock()
			}
			eventID++
			workerIdx = (workerIdx + 1) % p.NumWorkers
		}
	}()
}

// StartWorkers launches the fan-out worker pool.
func (p *Pipeline) StartWorkers(ctx context.Context) {
	for w := 0; w < p.NumWorkers; w++ {
		workerID := w
		ch := p.WorkerChans[workerID]
		p.wg.Add(1)
		go func(wid int, mych chan MetricEvent) {
			defer p.wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case evt := <-mych:
					// Simulate I/O or CPU (100-300ms)
					procT := time.Duration(100+rand.Intn(200)) * time.Millisecond
					time.Sleep(procT)
					// Send processed to Results
					select {
					case p.ResultChan <- ProcessedEvent{WorkerID: wid, Input: evt, OutVal: evt.ID * (wid + 1)}:
						// sent OK
					case <-ctx.Done():
						return
					}
				}
			}
		}(workerID, ch)
	}
}

// FanIn merged results into collector channel (ResultChan is used directly)
// A collector can read from ResultChan for aggregation/batching.

// CloseChannels closes all worker chans and result channel after shutdown
func (p *Pipeline) CloseChannels() {
	for _, ch := range p.WorkerChans {
		close(ch)
	}
	close(p.ResultChan)
}

// Wait waits for all goroutines to finish
func (p *Pipeline) Wait() {
	p.wg.Wait()
}

// DroppedSummary returns a copy of the dropped counts
func (p *Pipeline) DroppedSummary() []int {
	p.DropMu.Lock()
	defer p.DropMu.Unlock()
	res := make([]int, len(p.DroppedCounts))
	copy(res, p.DroppedCounts)
	return res
}

// Collector reads from resultChan for further aggregation
func (p *Pipeline) CollectResults(ctx context.Context, results *[]ProcessedEvent) {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case res, ok := <-p.ResultChan:
				if !ok {
					return
				}
				if results != nil {
					*results = append(*results, res)
				}
			}
		}
	}()
}
