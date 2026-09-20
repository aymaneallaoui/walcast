package app

import (
	"math"
	"testing"
	"time"
)

func TestBackoff(t *testing.T) {
	t.Run("stays within half the capped delay and the cap", func(t *testing.T) {
		const minDelay, maxDelay = 100 * time.Millisecond, 2 * time.Second
		for attempt := range 64 {
			d := backoff(attempt, minDelay, maxDelay)
			if d < minDelay/2 || d > maxDelay {
				t.Fatalf("attempt %d: delay %v outside [%v, %v]", attempt, d, minDelay/2, maxDelay)
			}
		}
	})

	t.Run("grows until it reaches the cap", func(t *testing.T) {
		const minDelay, maxDelay = time.Second, 30 * time.Second
		if d := backoff(0, minDelay, maxDelay); d > minDelay {
			t.Fatalf("first delay %v above min %v", d, minDelay)
		}
		if d := backoff(10, minDelay, maxDelay); d < maxDelay/2 {
			t.Fatalf("late delay %v below half the cap", d)
		}
	})

	t.Run("does not overflow with huge inputs", func(t *testing.T) {
		const maxDelay = time.Duration(math.MaxInt64)
		if d := backoff(1000, time.Hour, maxDelay); d <= 0 {
			t.Fatalf("delay overflowed to %v", d)
		}
	})
}
