package atoms

type AtomicMetricName string

const (

	// reserved internal metrics
	TOTAL_FINAL_JOB_SIGNALS_DISPATCHED      AtomicMetricName = "total_final_job_signals_dispatched"
	TOTAL_FINAL_JOB_SIGNALS_PLACED_ON_QUEUE AtomicMetricName = "total_final_job_signals_placed_on_queue"

	// global metrics
	TOTAL_DISPATCHERS                        AtomicMetricName = "total_dispatchers"
	TOTAL_ACTIVE_WORKERS                     AtomicMetricName = "total_active_workers"
	TOTAL_WORKERS                            AtomicMetricName = "total_workers"
	TOTAL_AVAILABLE_WORKERS                  AtomicMetricName = "total_available_workers"
	TOTAL_JOBS                               AtomicMetricName = "total_jobs"
	TOTAL_JOBS_DISPATCHED                    AtomicMetricName = "total_jobs_dispatched"
	TOTAL_FAILED_JOBS                        AtomicMetricName = "total_failed_jobs"
	TOTAL_JOBS_THROTTLED                     AtomicMetricName = "total_jobs_throttled"
	TOTAL_DURATION_THROTTLED_BY_RATE_LIMITER AtomicMetricName = "total_duration_throttled_by_rate_limiter"
	TOTAL_DURATION_PENDING_IN_QUEUE          AtomicMetricName = "total_duration_pending_in_queue"
	TOTAL_DURATION_DISPATCHER_BLOCKED        AtomicMetricName = "total_duration_dispatcher_blocked"
	TOTAL_JOB_EXECUTION                      AtomicMetricName = "total_job_execution"
	TOTAL_JOB_DURATION                       AtomicMetricName = "total_job_duration"
	TOTAL_EXPECTED_JOB_OVERHEAD_DURATION     AtomicMetricName = "total_expected_job_overhead_duration"
	TOTAL_UNEXPECTED_JOB_OVERHEAD_DURATION   AtomicMetricName = "total_unexpected_job_overhead_duration"
	TOTAL_JOB_OVERHEAD_DURATION              AtomicMetricName = "total_job_overhead_duration"
	TOTAL_RETRY_ATTEMPTS                     AtomicMetricName = "total_retry_attempts"

	// job metrics
	JOB_THROTTLE_COUNT              AtomicMetricName = "throttle_count"              // # of times job was throttled
	JOB_THROTTLE_DURATION           AtomicMetricName = "throttle_duration"           // duration of throttling
	JOB_DISPATCHER_BLOCKED_DURATION AtomicMetricName = "dispatcher_blocked_duration" // duration dispatcher was blocked
	JOB_DURATION                    AtomicMetricName = "job_duration"                // duration of job execution
)
