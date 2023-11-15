package gq

// control msg
type _ControlMsg int

// control event
type _ControlEvent int

// job status
type _JobStatus int

// component job status
type _ComponentStatus int

const (

	// Resevered wait group names
	DISPATCHER_RUN string = "dispatcher_run"
	DISPATCH_LOOP  string = "dispatch_loop"

	// Component names
	DISPATCHER    string = "DISPATCHER"
	WORKER        string = "WORKER"
	CONTROLLER    string = "CONTROLLER"
	LEDGER        string = "LEDGER"
	GQ_CONTROLLER string = "GQ"

	// Component status
	ONLINE _ComponentStatus = iota
	RUNNING
	OFFLINE

	// Job Status
	JOB_SUCCESS _JobStatus = iota
	JOB_FAILED
	JOB_PENDING
	JOB_RUNNING

	// Control Events
	CE_DISPATCHER_ONLINE _ControlEvent = iota
	CE_DISPATCHER_OFFLINE
	CE_WORKER_ONLINE
	CE_WORKER_OFFLINE
	CE_CONTROLLER_ONLINE

	// Reserved Final Job ID
	RESERVED_FINAL_JOB_ID   string = "GQ_RESERVED_FINAL_JOB_NAME"
	RESERVED_FINAL_JOB_NAME string = "GQ_RESERVED_FINAL_JOB_NAME"

	// Queue Status
	OPEN_FOR_JOBS int = iota
	CLOSED_FOR_JOBS

	// State Variables
	STATE_DISPATCHER_ONLINE string = "dispatcher_online"
	STATE_WORKER_ONLINE     string = "worker_online"
)
