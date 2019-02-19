package util

import (
	"context"
)

// NBContextClosed returns true if a context has been closed, false otherwise and doesn't block
func NBContextClosed(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
	}
	return false
}
