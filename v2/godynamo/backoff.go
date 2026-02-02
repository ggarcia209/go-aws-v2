// Package dynamo contains controls and objects for DynamoDB CRUD operations.
// Operations in this package are abstracted from all other application logic
// and are designed to be used with any DynamoDB table and any object schema.
// This file contains objects for implementing an exponential backoff
// algorithm for DynamoDB error handling.
package godynamo

import (
	"math"
	"math/rand"
	"time"
)

// Retries stores parameters for the exponential backoff algorithm.
// Attempt, Elapsed, MaxRetiresReached should always be initialized to 0, 0, false.
type Retries struct {
	base    int64
	cap     int64
	jitter  int64
	attempt int64
	elapsed int64
}

type FailConfig struct {
	Base   int64 `json:"base"`
	Cap    int64 `json:"cap"`
	Jitter int64 `json:"jitter"`
}

func (f *FailConfig) NewRetries() *Retries {
	return &Retries{base: f.Base, cap: f.Cap, jitter: f.Jitter}
}

func NewFailConfig(base, cap, jitter int64) *FailConfig {
	return &FailConfig{Base: base, Cap: cap, Jitter: jitter}
}

// DefaultFailConfig is the default configuration for the exponential backoff alogrithm
// with a base wait time of 50 miliseconds, and max wait time of 1 minute (60000 ms).
var DefaultFailConfig = &FailConfig{50, 60000, 250}

// ExponentialBackoff implements the exponential backoff algorithm for request retries
// and returns true when the max number of retries has been reached (r.Elapsed > r.Cap).
func (r *Retries) ExponentialBackoff() error {
	if r.elapsed >= r.cap {
		return NewMaxRetriesExceededError()
	}

	// exponential backoff with full jitter
	r.attempt += 1.0
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	jitter := rnd.Int63n(r.jitter)
	sleep := r.base * int64(math.Pow(2.0, float64(r.attempt)))
	wait := sleep + jitter

	if r.elapsed+wait > r.cap {
		// wait until cap is reached
		time.Sleep(time.Duration(wait - (wait + r.elapsed - r.cap)))
		r.elapsed = r.cap
		return nil
	}

	time.Sleep(time.Duration(wait) * time.Millisecond)
	r.elapsed += wait
	return nil
}
