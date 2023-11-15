package geras

import (
	"context"
	"sync"
	"time"

	"github.com/outofoffice3/geras-sandbox-pro/internal/atoms"
	"github.com/outofoffice3/geras-sandbox-pro/internal/controller"
	"github.com/outofoffice3/geras-sandbox-pro/pkg/ratelimiter"
)

// dispatcher to process jobs on the queue
type dispatcher interface {
	// run dispatcher
	run()
	// start job dispatch
	start()
	// wait for dispatcher
	wait()
	// return total number of workers
	getTotalWorkerCount() int32
	// return total jobs dispatched
	getTotalJobsDispatched() int32
	// return total jobs throttled
	getTotalJobsThrottled() int32
}

// implementation of Dispatcher interface
type _Dispatcher struct {
	ctx                     context.Context // execution context
	initialWorkerCount      int
	shutdownSignal          chan interface{}
	dispatcherWg            *sync.WaitGroup              // wait group for dispatcher
	workerWg                *sync.WaitGroup              // wait group for all workers
	rateLimiter             ratelimiter.RateLimiter      // rate limiter for dispatcher
	ledger                  ledger                       // reading / publishing job metadata to ledger
	jobListener             chan Job                     // listener for dispatching jobs
	controlMsgListener      chan controller.ControlMsg   // listener for control events
	controlEventSender      chan controller.ControlEvent // sending control events to controller
	dispatchableJobListener chan _DispatchableJob        // job listener for worker pool
	errorHandler            func(error)                  // error handler
	atomicMetrics           atoms.Atoms                  // atomic state of GQ
}

type dispatcherInput struct {
	ctx                      context.Context // execution context
	shutdownSignal           chan interface{}
	numWorkers               int                          // number of workers
	rateLimit                int                          // transactions allowed over rateLimitTimewWindow
	rateTimeWindow           int                          // time window for rate limiting (in seconds) i.e:  4 = 4 seconds
	jobListener              chan Job                     // listener for dispatching jobs
	controlMsgListener       chan controller.ControlMsg   // listener for control events for dispatcher
	workercontrolMsgListener []chan controller.ControlMsg // listener for control events for workers
	controlEventSender       chan controller.ControlEvent // sending control events to controller
	errorHandler             func(error)                  // error handler
	ledger                   ledger                       // reading / publish job metadata to ledger
	atomicMetrics            atoms.Atoms                  // atomic state of GQ
}

// create new Dispatcher
func newDispatcher(input dispatcherInput) (*_Dispatcher, error) {
	// check if worker count is valid
	if input.numWorkers <= 0 {
		sos.Errorf("invalid worker count, number of workers must be > 0")
		return nil, newError("number of workers must be greater than 0")
	}

	// create rate limiter
	timeWindow := input.rateTimeWindow * int(time.Duration.Seconds(time.Second))
	sos.Debugf("time window : [%v]", timeWindow)
	sos.Debugf("rate limit : [%v]", input.rateLimit)
	rlInput := ratelimiter.FixedWindowRateLimiterInput{
		Limit:      input.rateLimit,
		TimeWindow: timeWindow,
	}
	rl := ratelimiter.NewFixedWindowRateLimiter(rlInput)

	// initialize dispatcher
	dispatchableJobListener := make(chan _DispatchableJob) // create job listener for worker pool
	workerWg := &sync.WaitGroup{}                          // wait group for worker pool
	d := &_Dispatcher{
		dispatcherWg:            &sync.WaitGroup{},       // wait group for dispatch loop
		workerWg:                workerWg,                // wait group for worker pool
		dispatchableJobListener: dispatchableJobListener, // listener for worker poo
		rateLimiter:             rl,                      // rate limiter for backpressure
		initialWorkerCount:      input.numWorkers,
		ctx:                     input.ctx,                // execution context
		jobListener:             input.jobListener,        // listener for dispatching jobs
		controlMsgListener:      input.controlMsgListener, // listener for receivng control msgs
		controlEventSender:      input.controlEventSender, // sending control events to controller
		ledger:                  input.ledger,             // dependency injection for workers to publish job metadata
		atomicMetrics:           input.atomicMetrics,      // atomic state for Gq
		errorHandler:            input.errorHandler,       // error handler if any
		shutdownSignal:          input.shutdownSignal,
	}

	// initialize worker pool
	for i := 0; i < input.numWorkers; i++ {
		workerInput := workerInput{
			wg:                    workerWg,                          // shared wait group
			id:                    i,                                 // worker id
			ctx:                   input.ctx,                         // shared execution context
			dispatcherJobListener: input.jobListener,                 // worker id
			jobListener:           dispatchableJobListener,           // listener for receiving events from dispatcher
			control:               input.workercontrolMsgListener[i], // listener for receiving control msgs
			controlEventSender:    input.controlEventSender,          // sending control events to controller
			ledger:                input.ledger,                      // dependency injection to publish job metadata
			atomicMetrics:         input.atomicMetrics,               // atomic state for Gq
		}
		worker := NewWorker(workerInput) // create worker
		d.workerWg.Add(1)                // increment wait group counter for worker pool
		go worker.run()                  // start worker
		sos.Debugf("worker [%v] GO run()", worker.id)
	}
	sos.Debugf("new dispatcher created with [%v] workers", input.numWorkers)
	return d, nil
}

// start listening for control messages
func (d *_Dispatcher) run() {
	defer d.dispatcherWg.Done() // decrement wait group counter for dispatcher
	sos.Debugf("dispatcher control msg listener running...")
	for controlMsg := range d.controlMsgListener {
		d.dispatcherWg.Add(1)             // increment wait group counter
		go d.handleControlMsg(controlMsg) // handle control msg in go routine
		sos.Debugf("dispatcher GO handleControlMsg()")
	}
	sos.Debugf("dispatcher EXITED GO run()")
}

// handle control msgs
func (d *_Dispatcher) handleControlMsg(msg controller.ControlMsg) {
	defer d.dispatcherWg.Done() // decrement wait group counter
	switch msg.Name {
	case controller.INIT:
		{
			sos.Infof("[%v] control msg received by dispatcher from [%v]", msg.Name, msg.Sender)
			// send ONLINE event to controller
			controlEvent := controller.ControlEvent{
				Sender:   controller.DISPATCHER, // set sender to dispatcher
				SenderId: 0,
				Name:     controller.ONLINE, // set name to ONLINE
				Wg:       msg.Wg,
				Data:     nil,
			}
			d.controlEventSender <- controlEvent
			sos.Debugf("dispatcher sent [%v] event to controller", controller.ONLINE)

		}
	case controller.START:
		{
			sos.Infof("[%v] control msg received by dispatcher from [%v]", msg.Name, msg.Sender)
			d.dispatcherWg.Add(1) // increment dispatcher wg
			go d.start()          // start dispatch loop in go routine
			sos.Debugf("dispatcher GO start()")

		}
	case controller.FINALIZE:
		{
			sos.Infof("[%v] control msg received by dispatcher from [%v]", msg.Name, msg.Sender)
			msg.Wg.Done() // signal dispatcher has received FINALIZE signal
			msg.Wg.Wait() // wait for workers to signal they have completed their work
			shutdownWg := &sync.WaitGroup{}
			controlEvent := controller.ControlEvent{
				Sender:   controller.DISPATCHER, // set sender to dispatcher
				SenderId: 0,
				Name:     controller.OFFLINE, // set name to OFFLINE
				Wg:       shutdownWg,         // create new wait group for shutdown verification
				Data:     nil,
			}
			shutdownWg.Add(1)
			d.controlEventSender <- controlEvent // send OFFLINE event
			shutdownWg.Wait()
			close(d.dispatchableJobListener)
			close(d.jobListener)
			d.shutdownSignal <- shutdownSignal{}
			sos.Debugf("dispatcher exiting after sending [%v] event to controller", controlEvent.Name)
			sos.Debugf("dispatcher EXITED GO handleControlMsg()")
			return
		}
	}
}

// start dispatch loop
func (d *_Dispatcher) start() {
	defer d.dispatcherWg.Done() // decrement wait group counter for dispatch loop
	sos.Debugf("dispatcher job listener running...")
	for job := range d.jobListener {
		// check to make sure job is not the final job signal
		if job.GetJobId() != RESERVED_FINAL_JOB_ID {
			d.atomicMetrics.IncrementAtom(atoms.TOTAL_JOBS) // increment total job count
			d.handleJob(job)                                // handle job
			continue
		}
		d.handleFinalJob(job) // handle final job
	}
	sos.Debugf("dispatcher EXITED GO start()")
}

// handle final job
func (d *_Dispatcher) handleFinalJob(job Job) {
	finalJob, _ := job.(*_FinalJob)
	totalCount := int32(finalJob.totalCount)
	for {
		var totalJobDispatcherBlockedDuration time.Duration
		select {
		// dispatch job to the worker pool
		case d.dispatchableJobListener <- _DispatchableJob{
			job: job,
		}:
			d.atomicMetrics.IncrementAtom(atoms.TOTAL_FINAL_JOB_SIGNALS_DISPATCHED)
			sos.Debugf("final job signal [%v] dispatched to worker pool after [%v] milliseconds of being blocked", job.GetJobId(), totalJobDispatcherBlockedDuration)

			fjc, _ := d.atomicMetrics.GetAtom(atoms.TOTAL_FINAL_JOB_SIGNALS_DISPATCHED)
			if fjc != totalCount {
				sos.Debugf("final job count [%v] total workers [%v]", fjc, totalCount)
				return
			}
			sos.Debugf("final job count [%v] total workers [%v] dispatcher exiting", fjc, totalCount)
			return
		default:
			milliseconds := 1000
			sleepDuration := time.Duration(milliseconds) * time.Millisecond // sleep duration for current period
			totalJobDispatcherBlockedDuration += sleepDuration              // total duration dispatcher is blocked
			time.Sleep(sleepDuration)                                       // sleep for 2 milliseconds and try again
		}
	}
}

// handle job
func (d *_Dispatcher) handleJob(job Job) {
	sos.Debugf("job [%v] received by dispatcher", job.GetJobId())
	entry, _ := d.ledger.getEntry(job.GetJobId()) // check if entry is there
	//sos.Debugf("leger entry found [%+v] [%v]", entry, ok)
	entry.jobMetadata.timePickedUpByDispatcher = time.Now() // save time job was picked up by dispatcher
	// verify dispatcher has not exceeded rate limit
	var (
		totalRateLimiterSleepDuration     time.Duration
		totalJobDispatcherBlockedDuration time.Duration
		jobThrottled                      bool
	)
	jobThrottled = false
	retryRequest := true
	for retryRequest {
		rateLimitAllowed := d.rateLimiter.Allow()
		taw, _ := d.atomicMetrics.GetAtom(atoms.TOTAL_AVAILABLE_WORKERS)
		sos.Debugf("total available workers : [%v]", taw)
		workersAvailable := taw > int32(0)
		sos.Debugf("workers available : [%v]", workersAvailable)
		switch rateLimitAllowed && workersAvailable {
		case true:
			{
				sos.Debugf("rate limiter allowed request & worker is available for job")
				d.dispatchableJobListener <- _DispatchableJob{ // dispatch job to worker pool
					job: job,
				}
				entry.jobMetadata.timeDispatched = time.Now()                                                                     // save time was dispatched
				d.atomicMetrics.IncrementAtom(atoms.TOTAL_JOBS_DISPATCHED)                                                        // increment jobs dispatched
				entry.atomicState.AddValueToAtom(atoms.JOB_THROTTLE_DURATION, int32(totalRateLimiterSleepDuration))               // save job throttle duration
				entry.atomicState.AddValueToAtom(atoms.JOB_DISPATCHER_BLOCKED_DURATION, int32(totalJobDispatcherBlockedDuration)) // incremental total blocked duration
				retryRequest = false
				continue // break loop after dispatching
			}
		default:
			{
				if !rateLimitAllowed {
					jobThrottled = true
					milliseconds := 1000
					sleepDuration := time.Duration(milliseconds) * time.Millisecond // sleep duration for current period
					totalRateLimiterSleepDuration += sleepDuration                  // total sleep duration
					time.Sleep(sleepDuration)                                       // sleep and proceed once rate limiter allows request
					retryRequest = true
					entry.atomicState.IncrementAtom(atoms.JOB_THROTTLE_COUNT) // increment throttle count for job
					d.atomicMetrics.IncrementAtom(atoms.TOTAL_RETRY_ATTEMPTS) // increment total retry attempts
				}
				if !workersAvailable {
					milliseconds := 1000
					sleepDuration := time.Duration(milliseconds) * time.Millisecond // sleep duration for current period
					totalJobDispatcherBlockedDuration += sleepDuration              // total duration dispatcher is blocked
					time.Sleep(sleepDuration)                                       // sleep for 2 milliseconds and try again
					retryRequest = true
				}
				if retryRequest {
					entry.atomicState.IncrementAtom(atoms.JOB_THROTTLE_COUNT) // increment throttle count for job
				}
			}
		}
	}
	if jobThrottled {
		d.atomicMetrics.IncrementAtom(atoms.TOTAL_JOBS_THROTTLED) // increment total jobs throttled
	}
}

// wait for dispatcher
func (d *_Dispatcher) wait() {
	d.workerWg.Wait()
	d.dispatcherWg.Wait()
}

// get total worker count
func (d *_Dispatcher) getTotalWorkerCount() int32 {
	return int32(d.initialWorkerCount)
}

// get total jobs dispatched
func (d *_Dispatcher) getTotalJobsDispatched() int32 {
	result, _ := d.atomicMetrics.GetAtom(atoms.TOTAL_JOBS_DISPATCHED)
	return result
}

// get total jobs throttled
func (d *_Dispatcher) getTotalJobsThrottled() int32 {
	result, _ := d.atomicMetrics.GetAtom(atoms.TOTAL_JOBS_THROTTLED)
	return result
}
