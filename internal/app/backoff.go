package app

import (
	"math/rand/v2"
	"time"
)

func backoff(attempt int, minDelay, maxDelay time.Duration) time.Duration {
	d := minDelay
	for range attempt {
		if d >= maxDelay/2 {
			d = maxDelay
			break
		}
		d *= 2
	}
	d = min(d, maxDelay)
	half := d / 2
	return half + rand.N(half+1)
}
