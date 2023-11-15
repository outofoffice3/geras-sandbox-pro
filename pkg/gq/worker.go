package gq

import (
	"context"
	"sync"
	"time"

	"github.com/outofoffice3/geras-sandbox-pro/internal/atoms"
	"github.com/outofoffice3/geras-sandbox-pro/internal/controller"
)

type _Worker struct {
	ctx                   context.Context              // execution context
	wg                    *sync.WaitGroup              // wait group for workers
	id                    int                          // worker id
	dispatcherJobListener chan Job                     // channel for sending finish job signal to dispatcher
	jobListener           chan _DispatchableJob        // channel for receiving jobs
	control               chan controller.ControlMsg   // channel for receiving control messages
	controlEventSender    chan controller.ControlEvent // sending control events to controller
	ledger                ledger                       // dependency injection for adding job metadata to ledger
	atomicMetrics         atoms.Atoms                  // atomic state for GQ
	errorHandler          func(error)                  // handler for failed jobs
}

type workerInput struct {
	ctx                   context.Context              // execution context
	wg                    *sync.WaitGroup              // wait group for workers
	id                    int                          // worker id
	dispatcherJobListener chan Job                     // channel for sending finish job signal to dispatcher
	jobListener           chan _DispatchableJob        // channel for receiving jobs
	control               chan controller.ControlMsg   // channel for receiving control messages
	controlEventSender    chan controller.ControlEvent // sending control events to controller
	ledger                ledger                       // dependency injection for adding metadata to ledger
	atomicMetrics         atoms.Atoms                  // atomic state for GQ
	errorHandler          func(error)                  // handler for failed jobs
}

// create new worker
func NewWorker(input workerInput) *_Worker {
	return &_Worker{
		ctx:                   input.ctx,
		id:                    input.id,
		wg:                    input.wg,
		dispatcherJobListener: input.dispatcherJobListener,
		jobListener:           input.jobListener,
		control:               input.control,
		controlEventSender:    input.controlEventSender,
		ledger:                input.ledger,
		atomicMetrics:         input.atomicMetrics,
		errorHandler:          input.errorHandler,
	}
}

// run worker
func (w *_Worker) run() {
	defer w.wg.Done() // decrement wait group when function terminates
	sos.Infof("worker [%v] listening for control msgs...", w.id)
	for controlMsg := range w.control {
		go w.handleControlMsg(controlMsg) // handle control messages)
		w.wg.Add(1)                       // increment wait group for control message handling
	}
	sos.Debugf("worker [%v] run() go routine exited", w.id)
}

// handle control msgs
func (w *_Worker) handleControlMsg(msg controller.ControlMsg) {
	defer w.wg.Done()
	sos.Infof("worker [%v] received control message", w.id)
	switch msg.Name {
	case controller.INIT:
		{
			sos.Infof("[%v] received by worker [%v]", msg.Name, w.id)
			controlEvent := controller.ControlEvent{
				Sender:   controller.WORKER,
				SenderId: w.id,
				Name:     controller.ONLINE,
				Wg:       msg.Wg,
				Data:     nil,
			}
			w.controlEventSender <- controlEvent // send control event to controller
			sos.Infof("worker [%v] sent control event [%v] to controller", w.id, controller.ONLINE)
			go w.start()
			w.wg.Add(1) // increment wait group for worker job processing loop
			break
		}
	case controller.FINALIZE:
		{
			sos.Infof("[%v] received by worker [%v]", msg.Name, w.id)
			// create final job to clear buffer
			totalCount, _ := msg.Params.(int) // type assert total count
			fj := _FinalJob{
				wg:         msg.Wg,
				jobId:      RESERVED_FINAL_JOB_ID,
				jobName:    RESERVED_FINAL_JOB_NAME,
				totalCount: totalCount,
			}
			// send final job to dispatcher
			w.dispatcherJobListener <- &fj
			w.atomicMetrics.IncrementAtom(atoms.TOTAL_FINAL_JOB_SIGNALS_PLACED_ON_QUEUE)
			sos.Infof("worker [%v] sent final job to dispatcher", w.id)
			break
		}
	case controller.RESERVED_SHUTDOWN_MSG:
		{
			sos.Infof("[%v] received by worker [%v]", msg.Name, w.id)
			defer msg.Wg.Done() // acknowledge reserved shutdown signal
			sos.Infof("worker [%v] control msg listener exiting", w.id)
			return
		}
	}
}

// start worker pool
func (w *_Worker) start() {
	defer w.wg.Done()
	// start worker job processing loop
	for dispatchableJob := range w.jobListener {
		job := dispatchableJob.job
		// process all jobs except final job
		if job.GetJobId() != RESERVED_FINAL_JOB_ID {
			w.atomicMetrics.DecrementAtom(atoms.TOTAL_AVAILABLE_WORKERS)
			w.handleJob(&dispatchableJob)
			w.atomicMetrics.IncrementAtom(atoms.TOTAL_AVAILABLE_WORKERS)
			continue
		}
		// process final job
		w.atomicMetrics.DecrementAtom(atoms.TOTAL_AVAILABLE_WORKERS)
		w.handleFinalJob(job)
		return
	}
	sos.Debugf("worker [%v] start() go routine exited")
}

// handle final job
func (w *_Worker) handleFinalJob(job Job) {
	sos.Debugf("worker [%v] received final job signal", w.id)
	_ = job.Do() // process final job
	sos.Debugf("worker [%v] exiting job processing loop", w.id)
}

// handle job
func (w *_Worker) handleJob(dj *_DispatchableJob) {
	job := dj.job // get job from dispatchable job
	sos.Debugf("worker [%v] received job [%+v]", w.id, job)
	entry, _ := w.ledger.getEntry(job.GetJobId())
	entry.workerId = w.id                      // save worker id
	entry.jobMetadata.timeStarted = time.Now() // save time job was started
	entry.jobMetadata.status = JOB_RUNNING     // set job status to running
	err := job.Do()                            // process job

	// handle failed job
	if err != nil {
		sos.Errorf("worker [%v] failed to process job [%v]: %v", w.id, job.GetJobId(), err)
		entry.jobMetadata.errMsg = err.Error()       // save error message
		entry.jobMetadata.status = JOB_FAILED        // set job status to failed
		entry.jobMetadata.timeCompleted = time.Now() // save time job completed

		// call error handler if provided
		if w.errorHandler != nil {
			w.errorHandler(err)
		}

		// send failed job event
		failedJobEvent := controller.ControlEvent{
			Sender:   controller.WORKER,
			SenderId: w.id,
			Name:     controller.JOB_FAILED,
			Data:     job,
		}
		w.controlEventSender <- failedJobEvent
		return
	}
	entry.jobMetadata.status = JOB_SUCCESS       // set job status to success
	entry.jobMetadata.timeCompleted = time.Now() // save time job completed
}
