package geras

import (
	"sync"
)

type Job interface {
	// logic for executing the job
	Do() error
	// set job name
	SetJobName(name string)
	// get job name
	GetJobName() string
	// Set job metadata id
	SetJobId(id string) error
	// Get job metadata id
	GetJobId() string
}

// dispatcher wrapper for job execution
type _DispatchableJob struct {
	job Job
}

// signal that jobs have finished
type _FinalJob struct {
	wg         *sync.WaitGroup // wait group for shutdown
	jobId      string          // job id
	jobName    string          // job name
	totalCount int
}

// business logic for job
func (ej *_FinalJob) Do() error {
	defer ej.wg.Done()
	sos.Debugf("FINAL JOB EXECUTED")
	return nil
}

// set job name
func (ej *_FinalJob) SetJobName(name string) {
	ej.jobName = name
}

// get job name
func (ej *_FinalJob) GetJobName() string {
	return ej.jobName
}

// set job metadata id
func (ej *_FinalJob) SetJobId(id string) error {
	// if the job is already set return error
	if ej.jobId != "" {
		sos.Errorf("job id already set for job %s", ej.jobId)
		return newError("job id already set for job [" + ej.jobId + "]")
	}
	ej.jobId = RESERVED_FINAL_JOB_ID
	return nil
}

// get job id
func (ej *_FinalJob) GetJobId() string {
	return ej.jobId
}
