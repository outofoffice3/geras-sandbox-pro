package ratelimiter

import (
	"time"

	"github.com/outofoffice3/common/logger"
)

type RateLimiter interface {
	Allow() bool
}

type FixedWindowRateLimiterInput struct {
	Limit      int // maximum number of requests allowed in a time window
	TimeWindow int // time window in seconds
}

type FixedWindowRateLimiter struct {
	requests    int           // number of requests made in the current time window
	limit       int           // maximum number of requests allowed in a time window
	timeWindow  time.Duration // length of the time window in which requests are allowed
	windowStart time.Time     // start of time window
}

var sos logger.Logger

func Init(level logger.LogLevel) {
	sos = logger.NewConsoleLogger(level)
	sos.Infof("rate limiter init success")
}

func NewFixedWindowRateLimiter(input FixedWindowRateLimiterInput) *FixedWindowRateLimiter {
	return &FixedWindowRateLimiter{
		limit:       input.Limit,
		timeWindow:  time.Duration(input.TimeWindow) * time.Second,
		windowStart: time.Now(),
	}
}

func (r *FixedWindowRateLimiter) Allow() bool {
	now := time.Now() // get current time
	sos.Debugf("current time [%v]", now)

	// If the current time window has expired, reset the request count
	if now.Sub(r.windowStart) >= r.timeWindow {
		sos.Debugf("time window expired [%v]", now.Sub(r.windowStart))
		r.requests = 0      // reset request count
		r.windowStart = now // reset window start time to now
		sos.Debugf("request count reset to [%v]", r.requests)
	}

	// if request count is below limit allow request
	if r.requests < r.limit {
		sos.Debugf("request [%v] are below limit of [%v]", r.requests, r.limit)
		r.requests++ // increment request count
		sos.Debugf("request count incremented to [%v]", r.requests)
		return true
	}

	sos.Debugf("request [%v] are above limit of [%v]", r.requests, r.limit)
	return false // rate limit exceeded
}
