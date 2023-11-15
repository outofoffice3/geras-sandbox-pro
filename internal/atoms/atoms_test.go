package atoms

import (
	"testing"

	"github.com/outofoffice3/common/logger"
	"github.com/stretchr/testify/assert"
)

func TestAtoms(t *testing.T) {
	a := assert.New(t)
	Init(logger.LogLevelDebug)
	testAtom := NewAtoms()
	a.NotNil(testAtom)

	testCases := []struct {
		name          AtomicMetricName
		expectedValue int32
		description   string
	}{
		{TOTAL_AVAILABLE_WORKERS, int32(0), "total available workers init"},
		{TOTAL_FAILED_JOBS, int32(0), "total failed jobs init"},
		{TOTAL_JOBS_DISPATCHED, int32(0), "total jobs dispatched init"},
		{TOTAL_WORKERS, int32(0), "total workers init"},
		{TOTAL_JOBS_THROTTLED, int32(0), "total jobs throttled init"},
		{TOTAL_DURATION_THROTTLED_BY_RATE_LIMITER, int32(0), "total duration throttled by rate limiter init"},
		{TOTAL_DURATION_PENDING_IN_QUEUE, int32(0), "total duration pending in queue init"},
		{TOTAL_DURATION_DISPATCHER_BLOCKED, int32(0), "total duration dispatcher blocked init"},
		{TOTAL_JOB_EXECUTION, int32(0), "total job execution init"},
		{TOTAL_JOB_DURATION, int32(0), "total job duration init"},
		{TOTAL_EXPECTED_JOB_OVERHEAD_DURATION, int32(0), "total expected job overhead duration init"},
		{TOTAL_UNEXPECTED_JOB_OVERHEAD_DURATION, int32(0), "total unexpected job overhead duration init"},
		{TOTAL_JOB_OVERHEAD_DURATION, int32(0), "total job overhead duration init"},
		{TOTAL_JOBS, int32(0), "total jobs init"},
		{TOTAL_RETRY_ATTEMPTS, int32(0), "total retry attempts init"},
		{TOTAL_DISPATCHERS, int32(0), "total dispatchers init"},
		{TOTAL_ACTIVE_WORKERS, int32(0), "total active workers init"},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			result, err := testAtom.GetAtom(tc.name)
			a.NoError(err, "should not be an error")
			a.Equal(tc.expectedValue, result, "should equal 0")
		})
	}

	incrementTestCases := []struct {
		name          AtomicMetricName
		expectedValue int32
		description   string
	}{
		{TOTAL_AVAILABLE_WORKERS, int32(1), "total available workers increment"},
		{TOTAL_FAILED_JOBS, int32(1), "total failed jobs increment"},
		{TOTAL_JOBS_DISPATCHED, int32(1), "total jobs dispatched increment"},
		{TOTAL_WORKERS, int32(1), "total workers increment"},
		{TOTAL_JOBS_THROTTLED, int32(1), "total jobs throttled increment"},
		{TOTAL_DURATION_THROTTLED_BY_RATE_LIMITER, int32(1), "total duration throttled by rate limiter increment"},
		{TOTAL_DURATION_PENDING_IN_QUEUE, int32(1), "total duration pending in queue increment"},
		{TOTAL_DURATION_DISPATCHER_BLOCKED, int32(1), "total duration dispatcher blocked increment"},
		{TOTAL_JOB_EXECUTION, int32(1), "total job execution increment"},
		{TOTAL_JOB_DURATION, int32(1), "total job duration increment"},
		{TOTAL_EXPECTED_JOB_OVERHEAD_DURATION, int32(1), "total expected job overhead duration increment"},
		{TOTAL_UNEXPECTED_JOB_OVERHEAD_DURATION, int32(1), "total unexpected job overhead duration increment"},
		{TOTAL_JOB_OVERHEAD_DURATION, int32(1), "total job overhead duration increment"},
		{TOTAL_JOBS, int32(1), "total jobs increment"},
		{TOTAL_RETRY_ATTEMPTS, int32(1), "total retry attempts increment"},
		{TOTAL_DISPATCHERS, int32(1), "total dispatchers increment"},
		{TOTAL_ACTIVE_WORKERS, int32(1), "total active workers increment"},
	}
	for _, tc := range incrementTestCases {
		t.Run(tc.description, func(t *testing.T) {
			err := testAtom.IncrementAtom(tc.name)
			a.NoError(err, "should not be an error")
			result, _ := testAtom.GetAtom(tc.name)
			a.Equal(tc.expectedValue, result, "should equal 0")
		})
	}

	decrementTestCases := []struct {
		name          AtomicMetricName
		expectedValue int32
		description   string
	}{
		{TOTAL_AVAILABLE_WORKERS, int32(0), "total available workers decrement"},
		{TOTAL_FAILED_JOBS, int32(0), "total failed jobs decrement"},
		{TOTAL_JOBS_DISPATCHED, int32(0), "total jobs dispatched decrement"},
		{TOTAL_WORKERS, int32(0), "total workers decrement"},
		{TOTAL_JOBS_THROTTLED, int32(0), "total jobs throttled decrement"},
		{TOTAL_DURATION_THROTTLED_BY_RATE_LIMITER, int32(0), "total duration throttled by rate limiter decrement"},
		{TOTAL_DURATION_PENDING_IN_QUEUE, int32(0), "total duration pending in queue decrement"},
		{TOTAL_DURATION_DISPATCHER_BLOCKED, int32(0), "total duration dispatcher blocked decrement"},
		{TOTAL_JOB_EXECUTION, int32(0), "total job execution decrement"},
		{TOTAL_JOB_DURATION, int32(0), "total job duration decrement"},
		{TOTAL_EXPECTED_JOB_OVERHEAD_DURATION, int32(0), "total expected job overhead duration decrement"},
		{TOTAL_UNEXPECTED_JOB_OVERHEAD_DURATION, int32(0), "total unexpected job overhead duration decrement"},
		{TOTAL_JOB_OVERHEAD_DURATION, int32(0), "total job overhead duration decrement"},
		{TOTAL_JOBS, int32(0), "total jobs decrement"},
		{TOTAL_RETRY_ATTEMPTS, int32(0), "total retry attempts decrement"},
		{TOTAL_DISPATCHERS, int32(0), "total dispatchers decrement"},
		{TOTAL_ACTIVE_WORKERS, int32(0), "total active workers decrement"},
	}
	for _, tc := range decrementTestCases {
		t.Run(tc.description, func(t *testing.T) {
			err := testAtom.DecrementAtom(tc.name)
			a.NoError(err, "should not be an error")
			result, err := testAtom.GetAtom(tc.name)
			a.NoError(err, "should not be an error")
			a.Equal(tc.expectedValue, result, "should equal 0")
		})
	}

	addTestCases := []struct {
		name          AtomicMetricName
		valueToAdd    int32
		expectedValue int32
		description   string
	}{
		{TOTAL_AVAILABLE_WORKERS, int32(5), int32(5), "total available workers addition"},
		{TOTAL_FAILED_JOBS, int32(5), int32(5), "total failed jobs addition"},
		{TOTAL_JOBS_DISPATCHED, int32(5), int32(5), "total jobs dispatched addition"},
		{TOTAL_WORKERS, int32(5), int32(5), "total workers addition"},
		{TOTAL_JOBS_THROTTLED, int32(5), int32(5), "total jobs throttled addition"},
		{TOTAL_DURATION_THROTTLED_BY_RATE_LIMITER, int32(5), int32(5), "total duration throttled by rate limiter addition"},
		{TOTAL_DURATION_PENDING_IN_QUEUE, int32(5), int32(5), "total duration pending in queue addition"},
		{TOTAL_DURATION_DISPATCHER_BLOCKED, int32(5), int32(5), "total duration dispatcher blocked addition"},
		{TOTAL_JOB_EXECUTION, int32(5), int32(5), "total job execution addition"},
		{TOTAL_JOB_DURATION, int32(5), int32(5), "total job duration addition"},
		{TOTAL_EXPECTED_JOB_OVERHEAD_DURATION, int32(5), int32(5), "total expected job overhead duration addition"},
		{TOTAL_UNEXPECTED_JOB_OVERHEAD_DURATION, int32(5), int32(5), "total unexpected job overhead duration addition"},
		{TOTAL_JOB_OVERHEAD_DURATION, int32(5), int32(5), "total job overhead duration addition"},
		{TOTAL_JOBS, int32(5), int32(5), "total jobs addition"},
		{TOTAL_RETRY_ATTEMPTS, int32(5), int32(5), "total retry attempts addition"},
		{TOTAL_DISPATCHERS, int32(5), int32(5), "total dispatchers addition"},
		{TOTAL_ACTIVE_WORKERS, int32(5), int32(5), "total active workers addition"},
	}
	for _, tc := range addTestCases {
		t.Run(tc.description, func(t *testing.T) {
			err := testAtom.AddValueToAtom(tc.name, tc.valueToAdd)
			a.NoError(err)
			result, _ := testAtom.GetAtom(tc.name)
			a.Equal(tc.expectedValue, result, "should equal int32(5)")
		})
	}

	taw := int32(0)
	err := testAtom.AddAtom(TOTAL_ACTIVE_WORKERS, &taw)
	a.Error(err, "TOTAL_ACTIVE_WORKERS already exists")

	value, err := testAtom.GetAtom(AtomicMetricName("invalid"))
	a.Error(err, "invalid atom name")
	a.Equal(int32(0), value, "should equal 0")
	err = testAtom.IncrementAtom(AtomicMetricName("invalid"))
	a.Error(err, "invalid atom name")
	err = testAtom.DecrementAtom(AtomicMetricName("invalid"))
	a.Error(err, "invalid atom name")
	err = testAtom.AddValueToAtom(AtomicMetricName("invalid"), int32(0))
	a.Error(err, "invalid atom name")
}
