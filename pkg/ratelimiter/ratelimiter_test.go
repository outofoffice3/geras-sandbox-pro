package ratelimiter

import (
	"testing"
	"time"

	"github.com/outofoffice3/common/logger"
	"github.com/stretchr/testify/assert"
)

func TestRateLimiter(t *testing.T) {
	a := assert.New(t)
	Init(logger.LogLevelDebug)
	config := FixedWindowRateLimiterInput{
		Limit:      1,
		TimeWindow: 5,
	}
	rl := NewFixedWindowRateLimiter(config)
	a.True(rl.Allow())
	a.False(rl.Allow())
	time.Sleep(5 * time.Second) // sleep for 5 seconds
	a.True(rl.Allow())
}
