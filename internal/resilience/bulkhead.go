package resilience

import "sync/atomic"

// Bulkhead is a non-blocking concurrency limit. Each request class (browse,
// reserve, pay) gets its own pool so a flood of one cannot starve the others,
// and excess work is rejected immediately instead of queueing and timing out.
type Bulkhead struct {
	Name string
	sem  chan struct{}
	shed atomic.Int64
}

func NewBulkhead(name string, size int) *Bulkhead {
	return &Bulkhead{Name: name, sem: make(chan struct{}, size)}
}

func (b *Bulkhead) TryAcquire() bool {
	select {
	case b.sem <- struct{}{}:
		return true
	default:
		b.shed.Add(1)
		return false
	}
}

func (b *Bulkhead) Release() { <-b.sem }

func (b *Bulkhead) InFlight() int { return len(b.sem) }
func (b *Bulkhead) Shed() int64   { return b.shed.Load() }
