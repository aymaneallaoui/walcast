package replication

import (
	"math"
	"testing"
)

func TestSlotHealth(t *testing.T) {
	h := newSlotHealth()
	t.Run("unknown until the first poll", func(t *testing.T) {
		if !math.IsNaN(math.Float64frombits(h.lag.Load())) || h.is("reserved") != 0 {
			t.Fatalf("lag=%v reserved=%v, want NaN and 0", math.Float64frombits(h.lag.Load()), h.is("reserved"))
		}
	})
	t.Run("exactly one status is set", func(t *testing.T) {
		status := "extended"
		h.status.Store(&status)
		for _, s := range slotStatuses {
			if want := map[bool]float64{true: 1, false: 0}[s == status]; h.is(s) != want {
				t.Fatalf("is(%s) = %v, want %v", s, h.is(s), want)
			}
		}
	})
	t.Run("reset forgets what a dead watcher last saw", func(t *testing.T) {
		h.safe.Store(math.Float64bits(42))
		h.reset()
		if !math.IsNaN(math.Float64frombits(h.safe.Load())) || h.is("extended") != 0 {
			t.Fatal("reset kept a stale value")
		}
	})
}
