package backoff

import (
	"math/rand/v2"
	"time"
)

// Delay doubles minDelay per attempt up to maxDelay, then picks a random point in the upper half
// so that many clients failing together do not retry in lockstep.
func Delay(attempt int, minDelay, maxDelay time.Duration) time.Duration {
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
