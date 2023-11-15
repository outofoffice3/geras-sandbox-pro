package geras

// job status
type _JobStatus int

// component job status
type _ComponentStatus int

const (

	// Resevered wait group names
	DISPATCHER_RUN string = "dispatcher_run"
	DISPATCH_LOOP  string = "dispatch_loop"

	// Component names
	WORKER string = "WORKER"

	// Component status
	ONLINE _ComponentStatus = iota
	RUNNING
	OFFLINE

	// Job Status
	JOB_SUCCESS _JobStatus = iota
	JOB_FAILED
	JOB_PENDING
	JOB_RUNNING

	// Reserved Final Job ID
	RESERVED_FINAL_JOB_ID   string = "GQ_RESERVED_FINAL_JOB_NAME"
	RESERVED_FINAL_JOB_NAME string = "GQ_RESERVED_FINAL_JOB_NAME"

	// Queue Status
	OPEN_FOR_JOBS int = iota
	CLOSED_FOR_JOBS
)
