package util

import (
	"context"
)

func NBContextClosed(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
	}
	return false
}
