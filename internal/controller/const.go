package controller

type ComponentName string
type ControlMsgName string
type ControlEventName string

const (
	DISPATCHER ComponentName = "DISPATCHER"
	WORKER     ComponentName = "WORKER"
	CONTROLLER ComponentName = "CONTROLLER"
	GQ         ComponentName = "GQ"

	INIT                  ControlMsgName = "CM_INIT"
	START                 ControlMsgName = "CM_START"
	FINALIZE              ControlMsgName = "CM_FINALIZE"
	RESERVED_SHUTDOWN_MSG ControlMsgName = "RESERVED_SHUTDOWN_MSG"

	OFFLINE                 ControlEventName = "CE_OFFLINE"
	ONLINE                  ControlEventName = "CE_ONLINE"
	RESERVED_SHUTDOWN_EVENT ControlEventName = "RESERVED_SHUTDOWN_EVENT"

	DISPATCHER_CREATE_FAILED ControlEventName = "CE_FAILED_JOB"

	JOB_FAILED ControlEventName = "CE_JOB_FAILED"
)
