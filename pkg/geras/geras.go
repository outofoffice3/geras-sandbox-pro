package geras

import (
	"context"
	_ "net/http/pprof"
	"strconv"
	"sync"
	"time"

	"github.com/outofoffice3/common/logger"
	"github.com/outofoffice3/geras-sandbox-pro/internal/atoms"
	"github.com/outofoffice3/geras-sandbox-pro/internal/controller"
	"github.com/outofoffice3/geras-sandbox-pro/pkg/ratelimiter"
)

var (
	sos logger.Logger
)

// buffered queue interface for processing jobs
type GQ interface {
	// Add jobs to queue
	AddJob(job Job) error
	// Start processing jobs
	Start() error
	// Stop processing jobs
	Stop() error
	// return length of queue
	Len() int
	// return capacity of job queue
	Cap() int
	// set error handler to deal with failed jobs
	SetErrorHandler(handler func(error))
	// return total number of failed jobs
	GetFailedJobsCount() int32
	// return total number of workers
	GetTotalWorkerCount() int32
	// return total number of jobs dispatched
	GetTotalJobsDispatched() int32
	// return total number of jobs throttled
	GetTotalJobsThrottled() int32
}

// implementation of GQ interface
type _Geras struct {
	status         int                        // status of GQ
	initWg         *sync.WaitGroup            // wait group for initialization
	cancelFunc     context.CancelFunc         // cancel function for execution context
	dispatcher     dispatcher                 // orchestrates job processing
	ledger         ledger                     // collects information about job execution
	controller     controller.Controller      // control channel for sending control msgs
	atomicMetrics  atoms.Atoms                // atomic state of GQ
	shutdownSignal chan interface{}           // shutdown signal
	queue          chan Job                   // buffered queue for incoming jobs
	reserved       chan controller.ControlMsg // reserved channel for controller
	errorHandler   func(error)                // error handler
}

type NewGerasInput struct {
	NumWorkers     int // number of workers
	QueueSize      int // size of job queue
	RateLimit      int // transactions allowed over rateTimeWindow
	RateTimeWindow int // time window for transactions allowed measured in seconds
}

func Init(level logger.LogLevel) {
	sos = logger.NewConsoleLogger(level) // initialize logger
	sos.Infof("GQ init started")
	// initialize atoms
	atoms.Init(level)
	controller.Init(level)
	ratelimiter.Init(logger.LogLevelInfo)
	sos.Infof("GQ init successfully")
}

// create new GQ
func NewGeras(input NewGerasInput) (GQ, error) {
	ctx, cancel := context.WithCancel(context.Background()) // create context
	defer cancel()                                          // make sure to cancel execution context after function completes
	buffQueue := make(chan Job, input.QueueSize)            // initialize buffered queue to hold incoming jobs
	shutdownSignal := make(chan interface{}, 1)             // initialize shutdown signal
	dispatcherShutdownSignal := make(chan interface{}, 1)
	atomicMetrics := atoms.NewAtoms() // initialize atomic metrics

	// create controller
	controlEventChannel := make(chan controller.ControlEvent, input.NumWorkers+1) // control event channel to send events to controller
	reserved := make(chan controller.ControlMsg, 1)                               // reserved channel for controller to receive control msgs
	gqController, _ := controller.NewController(controller.ControllerInput{
		Ctx:                      ctx,
		ShutdownSignal:           shutdownSignal,
		DispatcherShutdownSignal: dispatcherShutdownSignal,
		ControlMsgListener:       reserved,
		EventListener:            controlEventChannel,
		AtomicMetrics:            atomicMetrics,
	})

	// create control msg listeners for each component
	dispatcherControlMsgListener := make(chan controller.ControlMsg, 1)               // control channel listener for dispatcher
	workerControlMsgListeners := make([]chan controller.ControlMsg, input.NumWorkers) // control channel listener for workers

	// create control channels for workers
	for i := range workerControlMsgListeners {
		cc := make(chan controller.ControlMsg, 1) // create new channel for worker
		workerControlMsgListeners[i] = cc         // add channel to slice
		wId := createWorkerId(i)                  // create worker ID
		gqController.AddChannel(wId, cc)          // add channel to controller
	}

	// create ledger
	l := newLedger(ledgerInput{})

	// create dispatcher
	d, err := newDispatcher(dispatcherInput{
		numWorkers:               input.NumWorkers,     // set num of workers
		rateLimit:                input.RateLimit,      // transactions allowed over RateTimeWindow
		rateTimeWindow:           input.RateTimeWindow, // time window for transactions allowed measure in seconds
		shutdownSignal:           dispatcherShutdownSignal,
		ctx:                      ctx,                          // execution context
		jobListener:              buffQueue,                    // listener for jobs
		controlMsgListener:       dispatcherControlMsgListener, // listener for control msgs
		workercontrolMsgListener: workerControlMsgListeners,    // listener for worker control msgs
		controlEventSender:       controlEventChannel,          // send events to controller
		atomicMetrics:            atomicMetrics,                // atomic state for gq
		errorHandler:             nil,
		ledger:                   l,
	})
	// return errors
	if err != nil {
		// send error event to controller
		errorWg := &sync.WaitGroup{}
		errorWg.Add(1)
		gqController.GetEventListener() <- controller.ControlEvent{
			Sender:   controller.GERAS,
			SenderId: 0,
			Name:     controller.DISPATCHER_CREATE_FAILED,
			Wg:       errorWg,
			Data:     err,
		}
		errorWg.Wait() // wait for event to be processed
		return nil, newError("error creating dispatcher : [" + err.Error() + "]")
	}

	// add control msg listeners to GQ controller
	gqController.AddChannel(string(controller.DISPATCHER), dispatcherControlMsgListener) // add dispatcher to GQ controller
	gqController.AddChannel(string(controller.CONTROLLER), reserved)                     // add reserved channel for controller

	q := &_Geras{
		status:         OPEN_FOR_JOBS, // set status to open for jobs
		shutdownSignal: shutdownSignal,
		cancelFunc:     cancel,        // cancel function for execution context,
		dispatcher:     d,             // set dispatcher
		ledger:         l,             // set ledger
		controller:     gqController,  // set controller
		reserved:       reserved,      // reserved channel for controller
		queue:          buffQueue,     // set buffered queue
		atomicMetrics:  atomicMetrics, // atomic state of GQ
	}

	d.dispatcherWg.Add(1)
	go q.dispatcher.run() // run dispatcher
	sos.Debugf("dispatcher GO run()")

	// Initialize all components (controller, dispatcher and workers)
	q.initWg = &sync.WaitGroup{}
	controlMsg := controller.ControlMsg{
		Sender:   controller.GERAS, // set sender to GQ
		SenderId: 0,
		Name:     controller.INIT, // set msg name to INIT
		Wg:       q.initWg,
		Data:     nil,
	}
	q.controller.SendMsg(string(controller.CONTROLLER), controlMsg) // send INIT control msg to controller
	q.initWg.Add(input.NumWorkers + 1)
	q.initWg.Wait() // wait for initialization to complete
	sos.Debugf("GQ initialized")
	return q, nil
}

// add jobs
func (gq *_Geras) AddJob(job Job) error {
	gq.initWg.Wait() // wait for initialization to complete
	taw, _ := gq.atomicMetrics.GetAtom(atoms.TOTAL_ACTIVE_WORKERS)
	taw2, _ := gq.atomicMetrics.GetAtom(atoms.TOTAL_AVAILABLE_WORKERS)
	tw, _ := gq.atomicMetrics.GetAtom(atoms.TOTAL_WORKERS)
	sos.Debugf("total workers [%v] total active workers [%v] total available workers [%v]", tw, taw, taw2)
	if gq.status != OPEN_FOR_JOBS {
		return newError("cannot add job to closed GQ")
	}
	gq.queue <- job // add job to buffered queue
	input := _LedgerEntryInput{
		id:                       job.GetJobId(),
		jobName:                  job.GetJobName(),
		timeAddedToBufferedQueue: time.Now(),
		status:                   JOB_PENDING, // pending processing
	}
	gq.atomicMetrics.IncrementAtom(atoms.TOTAL_JOBS) // increment total jobs
	gq.ledger.addEntry(input)                        // add ledger entry
	return nil
}

// start processing jobs
func (gq *_Geras) Start() error {
	gq.initWg.Wait() // wait for initialization to complete
	taw, _ := gq.atomicMetrics.GetAtom(atoms.TOTAL_ACTIVE_WORKERS)
	taw2, _ := gq.atomicMetrics.GetAtom(atoms.TOTAL_AVAILABLE_WORKERS)
	tw, _ := gq.atomicMetrics.GetAtom(atoms.TOTAL_WORKERS)
	sos.Debugf("total workers [%v] total active workers [%v] total available workers [%v]", tw, taw, taw2)
	// send start control msg to dispatcher
	controlMsg := controller.ControlMsg{
		Name: controller.START,
		Data: nil,
	}
	gq.controller.SendMsg(string(controller.DISPATCHER), controlMsg)
	return nil
}

type shutdownSignal struct{}

// stop processing jobs
func (gq *_Geras) Stop() error {
	gq.status = CLOSED_FOR_JOBS // set status to closed for jobs
	gq.shutdownSignal <- shutdownSignal{}
	gq.cancelFunc() // send cancel signal to execution context
	sos.Debugf("GQ signaled to stop.  ctx cancellation signal sent")
	gq.controller.Wait() // wait for controller to finish
	gq.dispatcher.wait()
	sos.Infof("GQ stopped succesfully")
	return nil
}

// return capacity of job queue
func (gq *_Geras) Cap() int {
	return cap(gq.queue)
}

// return length of job queue
func (gq *_Geras) Len() int {
	gq.initWg.Wait()
	finalJobSignalCount, _ := gq.atomicMetrics.GetAtom(atoms.TOTAL_FINAL_JOB_SIGNALS_PLACED_ON_QUEUE)
	sos.Debugf("final job signal count [%v] length [%v]", finalJobSignalCount, len(gq.queue))
	return len(gq.queue) - int(finalJobSignalCount)
}

// set error handler to deal with failed jobs
func (gq *_Geras) SetErrorHandler(handler func(error)) {
	gq.errorHandler = handler
}

// return total number of failed jobs
func (gq *_Geras) GetFailedJobsCount() int32 {
	result, _ := gq.atomicMetrics.GetAtom(atoms.TOTAL_FAILED_JOBS)
	return result
}

// return total number of workers
func (gq *_Geras) GetTotalWorkerCount() int32 {
	return gq.dispatcher.getTotalWorkerCount()
}

// return total number of jobs dispatched
func (gq *_Geras) GetTotalJobsDispatched() int32 {
	result, _ := gq.atomicMetrics.GetAtom(atoms.TOTAL_JOBS_DISPATCHED)
	return result
}

// return total number of jobs throttled
func (gq *_Geras) GetTotalJobsThrottled() int32 {
	result, _ := gq.atomicMetrics.GetAtom(atoms.TOTAL_JOBS_THROTTLED)
	return result
}

func createWorkerId(workerId int) string {
	return WORKER + "[" + strconv.Itoa(int(workerId)) + "]"
}
