package controller

import (
	"context"
	_ "net/http/pprof"
	"runtime"
	"sync"
	"testing"

	"github.com/outofoffice3/common/logger"
	"github.com/outofoffice3/geras-sandbox-pro/internal/atoms"
	"github.com/stretchr/testify/assert"
)

func TestController(t *testing.T) {
	a := assert.New(t)
	Init(logger.LogLevelDebug)

	// create context w/ cancel
	ctx, cancel := context.WithCancel(context.Background())

	// create event listener and control msg listener
	eventListener := make(chan ControlEvent, 1)
	controlMsgListener := make(chan ControlMsg, 1)

	// create some test channels to add to controller
	testChan1 := make(chan ControlMsg, 1)
	testChan2 := make(chan ControlMsg, 1)

	// create atoms
	atom := atoms.NewAtoms()

	c, err := NewController(ControllerInput{
		Ctx:                ctx,
		EventListener:      eventListener,
		ControlMsgListener: controlMsgListener,
		AtomicMetrics:      atom,
	})
	a.NoError(err)
	a.NotNil(c)

	// add test channels to controller
	err = c.AddChannel("testchan1", testChan1)
	a.NoError(err)
	err = c.AddChannel("testchan2", testChan2)
	a.NoError(err)
	a.Equal(2, c.GetChannelCount(), "should be 2 channels added to controller")
	a.Equal(eventListener, c.GetEventListener())
	a.Equal(controlMsgListener, c.GetReservedChannel())

	// send INIT msg to controller
	initMsg := ControlMsg{
		Sender: "Tester",
		Name:   INIT,
	}
	c.controlMsgListener <- initMsg

	// handle init msgs
	go func() {
		msg := <-testChan1
		a.Equal(INIT, msg.Name)
		wg, ok := msg.Data.(*sync.WaitGroup)
		a.Equal(CONTROLLER, msg.Sender)
		a.True(ok)

		c.eventListener <- ControlEvent{
			Sender:   DISPATCHER,
			SenderId: 0,
			Name:     ONLINE,
			Data:     wg,
		}
	}()
	go func() {
		msg := <-testChan2
		a.Equal(INIT, msg.Name)
		wg, ok := msg.Data.(*sync.WaitGroup)
		a.Equal(CONTROLLER, msg.Sender)
		a.True(ok)

		c.eventListener <- ControlEvent{
			Sender:   WORKER,
			SenderId: 0,
			Name:     ONLINE,
			Data:     wg,
		}
	}()
	cancel() // send context cancellation

	c.Wait()
	// handle finalize msgs
	go func() {
		msg := <-testChan1
		a.Equal(FINALIZE, msg.Name)
		wg, ok := msg.Data.(*sync.WaitGroup)
		a.True(ok)
		wg.Done()
	}()
	go func() {
		msg := <-testChan2
		a.Equal(FINALIZE, msg.Name)
		wg, ok := msg.Data.(*sync.WaitGroup)
		a.True(ok)
		wg.Done()
	}()
	c.Wait()
	td, _ := c.AtomicMetrics.GetAtom(atoms.TOTAL_DISPATCHERS)
	tw, _ := c.AtomicMetrics.GetAtom(atoms.TOTAL_WORKERS)
	a.Equal(int32(0), td)
	a.Equal(int32(0), tw)
	runtime.GC()
	a.Equal(2, runtime.NumGoroutine())
}
