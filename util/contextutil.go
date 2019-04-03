package util

import "context"

// IsClosed returns true if a context has been closed, false otherwise.
// This function is non-blocking.
func IsClosed(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}
