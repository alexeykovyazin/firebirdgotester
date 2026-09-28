// Package safego runs functions on goroutines that must not take the whole
// process down: a panic in a background goroutine (net/http only recovers
// handler goroutines) would otherwise kill every running load session.
package safego

import (
	"fmt"
	"runtime/debug"
)

// Go runs fn on a new goroutine with a panic guard that logs and continues.
// name labels the goroutine in the log line.
func Go(name string, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("[panic] recovered in %s: %v\n%s\n", name, r, debug.Stack())
			}
		}()
		fn()
	}()
}
