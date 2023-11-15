package controller

import (
	"context"
	"sync"

	"github.com/outofoffice3/common/logger"
	"github.com/outofoffice3/geras-sandbox-pro/internal/atoms"
)

type ControlMsg struct {
	Sender   interface{}
	SenderId interface{}
	Wg       *sync.WaitGroup
	Name     ControlMsgName
	Data     interface{}
	Params   interface{}
}

type ControlEvent struct {
	Sender   interface{}
	SenderId interface{}
	Wg       *sync.WaitGroup
	Name     ControlEventName
	Data     interface{}
	Params   interface{}
}

type Controller interface {
	// send control msg to control channel
	SendMsg(id string, msg ControlMsg) error
	// add destination channel to Controller
	AddChannel(id string, dest chan ControlMsg) error
	// wait for controller to shutdown
	Wait()
	// total channels registered
	GetChannelCount() int
	// return event listener channel
	GetEventListener() chan ControlEvent
	// return reserved channel
	GetReservedChannel() chan ControlMsg
}

type _Controller struct {
	ctx                      context.Context            // exeuction context
	shutdownSignal           chan interface{}           // shutdown signal
	dispatcherShutdownSignal chan interface{}           // dispatcher shutdown signal
	wg                       *sync.WaitGroup            // wait group for controller run
	eventListener            chan ControlEvent          // event listener channel
	controlMsgListener       chan ControlMsg            // reserved channel to shutdown controller
	channels                 map[string]chan ControlMsg // channels to send msgs to
	AtomicMetrics            atoms.Atoms                // atomic state
}

var (
	sos logger.Logger
)

func Init(level logger.LogLevel) {
	sos = logger.NewConsoleLogger(level)
	sos.Infof("controller init success")
}

// send msg to channel
func (c *_Controller) SendMsg(id string, msg ControlMsg) error {
	if _, ok := c.channels[id]; !ok {
		return newError("invalid channel id")
	}
	c.channels[id] <- msg
	return nil
}

// add channel to Controller
func (c *_Controller) AddChannel(id string, dest chan ControlMsg) error {
	if _, ok := c.channels[id]; ok {
		return newError("channel id already exists")
	}
	c.channels[id] = dest
	sos.Debugf("channel %s added to controller", id)
	return nil
}

// get total channels
func (c *_Controller) GetChannelCount() int {
	return len(c.channels)
}

// run method
func (c *_Controller) listenForControlMsgs() {
	defer c.wg.Done() // decrement run wait group
	var finalizeWg *sync.WaitGroup
	sos.Debugf("controller control msg listener running...")
	for {
		select {
		case controlMsg, ok := <-c.controlMsgListener:
			{
				if !ok {
					sos.Debugf("control msg listener channel closed")
					return
				}
				c.wg.Add(1)
				go c.handleControlMsg(controlMsg)
			}
		case <-c.ctx.Done():
			{
				sos.Debugf("control msg listener received context cancellation")
				<-c.shutdownSignal // wait for shutdown signal
				sos.Debugf("control msg listener received shutdown signal")
				// send finalize control message to all channels
				finalizeWg = &sync.WaitGroup{}
				totalWorkers, _ := c.AtomicMetrics.GetAtom(atoms.TOTAL_WORKERS)
				sos.Debugf("total workers [%v]", totalWorkers)
				for id, channel := range c.channels {
					if id == string(CONTROLLER) {
						sos.Debugf("skipping controller channel for finalize msg")
						continue
					}
					channel <- ControlMsg{
						Sender:   CONTROLLER,
						SenderId: 0,
						Name:     FINALIZE,
						Wg:       finalizeWg,
						Data:     nil,
						Params:   totalWorkers,
					}
					finalizeWg.Add(1) // increment wait group counter
				}
				finalizeWg.Wait() // wait for resources to finalize their work
				sos.Debugf("control msg listener done waiting for finalize signals from all channels")

				// close all channels on the controller
				for id, channel := range c.channels {
					close(channel)
					sos.Debugf("closed channel [%v]", id)
				}

				// close controller event listener
				close(c.eventListener)
				sos.Debugf("controller closed event listener channel")
				<-c.dispatcherShutdownSignal
				sos.Debugf("received dispatcher shutdown signal...controller exiting")
				return // exit controller
			}
		}
	}
}

// handle control msg
func (c *_Controller) handleControlMsg(msg ControlMsg) {
	defer c.wg.Done() // decrement control handler wait group
	switch msg.Name {
	case INIT:
		{
			// send INIT control msgs to all channels
			for id, channel := range c.channels {
				if id == string(CONTROLLER) {
					sos.Debugf("skipping controller channel for INIT msg")
					continue
				}
				sos.Debugf("control msg listener received [%v] msg from [%v]", msg.Name, msg.Sender)
				initMsg := ControlMsg{
					Sender:   CONTROLLER,
					SenderId: 0,
					Name:     msg.Name,
					Wg:       msg.Wg,
					Data:     nil,
				}
				channel <- initMsg // send init msg to channel
				sos.Debugf("control msg listener sent [%v] control msg to [%v]", msg.Name, id)
			}
			sos.Debugf("control msg listener acknowledged [%v] control msg", msg.Name)
			return
		}
	}
}

// listen for events
func (c *_Controller) listenForEvents() {
	defer c.wg.Done() // decrement control event wait group counter
	sos.Debugf("controller event listener running...")
	for controlEvent := range c.eventListener {
		{
			sos.Debugf("event listener received event [%v] from [%v][%v]", controlEvent.Name, controlEvent.Sender, controlEvent.SenderId)
			if controlEvent.Name == RESERVED_SHUTDOWN_EVENT {
				defer controlEvent.Wg.Done() // decrement wait group counter
				sos.Debugf("event listener acknowledged [%v] event, event listener shutting down", controlEvent.Name)
				return // exit event listener
			}
			sos.Debugf("event listener handling control event [%v from [%v][%v]", controlEvent.Name, controlEvent.Sender, controlEvent.SenderId)
			c.wg.Add(1) // increment wait group counter
			go c.handleControlEvent(controlEvent)
		}
	}
	sos.Debugf("controller event listener go routine exited")
}

// handle events
func (c *_Controller) handleControlEvent(event ControlEvent) {
	defer c.wg.Done()
	switch event.Name {
	case DISPATCHER_CREATE_FAILED:
		{
			defer event.Wg.Done()
			sos.Debugf("event listener received event [%v] from [%v][%v]", event.Name, event.Sender, event.SenderId)
			// loop and close all channels on the controller
			for id, channel := range c.channels {
				close(channel)
				sos.Debugf("controller closed [%v] channel", id)
			}
			close(c.controlMsgListener)
			close(c.eventListener)
		}
	case JOB_FAILED:
		{
			sos.Debugf("event listener received event [%v] from [%v][%v]", event.Name, event.Sender, event.SenderId)
			c.AtomicMetrics.IncrementAtom(atoms.TOTAL_FAILED_JOBS) // increment job failed count
		}
	case OFFLINE:
		{
			switch event.Sender {
			case DISPATCHER:
				{
					sos.Debugf("event listener received event [%v] from [%v][%v]", event.Name, event.Sender, event.SenderId)
					defer event.Wg.Done()
					c.AtomicMetrics.DecrementAtom(atoms.TOTAL_DISPATCHERS) // decrement dispatcher count
				}
			case WORKER:
				{
					sos.Debugf("event listener received event [%v] from [%v][%v]", event.Name, event.Sender, event.SenderId)
					defer event.Wg.Done() // acknowledge event
					c.AtomicMetrics.DecrementAtom(atoms.TOTAL_ACTIVE_WORKERS)
				}
			}
		}
	case ONLINE:
		{
			switch event.Sender {
			case DISPATCHER:
				{
					sos.Debugf("event listener received event [%v] from [%v]", event.Name, event.Sender)
					defer event.Wg.Done()                                  // decrement finalize wait group counter
					c.AtomicMetrics.IncrementAtom(atoms.TOTAL_DISPATCHERS) // increment dispatcher count
				}
			case WORKER:
				{
					sos.Debugf("event listener received event [%v] from [%v][%v]", event.Name, event.Sender, event.SenderId)
					defer event.Wg.Done()
					c.AtomicMetrics.IncrementAtom(atoms.TOTAL_WORKERS)
					c.AtomicMetrics.IncrementAtom(atoms.TOTAL_ACTIVE_WORKERS)
					c.AtomicMetrics.IncrementAtom(atoms.TOTAL_AVAILABLE_WORKERS)
				}
			}
		}
	}
}

// wait for controller
func (c *_Controller) Wait() {
	c.wg.Wait() // wait for controller to shutdown
	sos.Debugf("controller done waiting")
}

// get event listener channel
func (c *_Controller) GetEventListener() chan ControlEvent {
	return c.eventListener
}

// get reserved channel
func (c *_Controller) GetReservedChannel() chan ControlMsg {
	return c.controlMsgListener
}

type ControllerInput struct {
	Ctx                      context.Context
	ShutdownSignal           chan interface{}
	DispatcherShutdownSignal chan interface{}
	EventListener            chan ControlEvent
	ControlMsgListener       chan ControlMsg
	AtomicMetrics            atoms.Atoms
}

// create new Controller
func NewController(input ControllerInput) (*_Controller, error) {
	c := &_Controller{
		ctx:                      input.Ctx,
		eventListener:            input.EventListener,
		shutdownSignal:           input.ShutdownSignal,
		dispatcherShutdownSignal: input.DispatcherShutdownSignal,
		AtomicMetrics:            input.AtomicMetrics,
		controlMsgListener:       input.ControlMsgListener,
		channels:                 make(map[string]chan ControlMsg),
		wg:                       &sync.WaitGroup{},
	}
	c.wg.Add(1)                 // increment wait group counter
	go c.listenForEvents()      // start event listener
	c.wg.Add(1)                 // increment wait group counte
	go c.listenForControlMsgs() // start controller
	return c, nil
}
