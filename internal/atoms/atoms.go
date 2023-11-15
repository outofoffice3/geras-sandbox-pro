package atoms

import (
	"errors"
	"sync/atomic"

	"github.com/outofoffice3/common/logger"
)

type Atoms interface {
	IncrementAtom(id AtomicMetricName) error
	DecrementAtom(id AtomicMetricName) error
	GetAtom(id AtomicMetricName) (int32, error)
	AddAtom(id AtomicMetricName, value *int32) error
	AddValueToAtom(id AtomicMetricName, value int32) error
}

type _Atom struct {
	atoms map[AtomicMetricName]*int32
}

var (
	sos logger.Logger
)

func Init(level logger.LogLevel) {
	sos = logger.NewConsoleLogger(level)
	sos.Infof("atom init success")
}

func NewAtoms() Atoms {
	atom := &_Atom{
		atoms: make(map[AtomicMetricName]*int32),
	}
	taw := int32(0)
	atom.AddAtom(TOTAL_AVAILABLE_WORKERS, &taw)
	tfj := int32(0)
	atom.AddAtom(TOTAL_FAILED_JOBS, &tfj)
	tjdd := int32(0)
	atom.AddAtom(TOTAL_JOBS_DISPATCHED, &tjdd)
	tw := int32(0)
	atom.AddAtom(TOTAL_WORKERS, &tw)
	tjt := int32(0)
	atom.AddAtom(TOTAL_JOBS_THROTTLED, &tjt)
	tdtbrl := int32(0)
	atom.AddAtom(TOTAL_DURATION_THROTTLED_BY_RATE_LIMITER, &tdtbrl)
	tdpiq := int32(0)
	atom.AddAtom(TOTAL_DURATION_PENDING_IN_QUEUE, &tdpiq)
	tddb := int32(0)
	atom.AddAtom(TOTAL_DURATION_DISPATCHER_BLOCKED, &tddb)
	tje := int32(0)
	atom.AddAtom(TOTAL_JOB_EXECUTION, &tje)
	tjd := int32(0)
	atom.AddAtom(TOTAL_JOB_DURATION, &tjd)
	tejod := int32(0)
	atom.AddAtom(TOTAL_EXPECTED_JOB_OVERHEAD_DURATION, &tejod)
	tuejod := int32(0)
	atom.AddAtom(TOTAL_UNEXPECTED_JOB_OVERHEAD_DURATION, &tuejod)
	tjod := int32(0)
	atom.AddAtom(TOTAL_JOB_OVERHEAD_DURATION, &tjod)
	tj := int32(0)
	atom.AddAtom(TOTAL_JOBS, &tj)
	tra := int32(0)
	atom.AddAtom(TOTAL_RETRY_ATTEMPTS, &tra)
	ctd := int32(0)
	atom.AddAtom(TOTAL_DISPATCHERS, &ctd)
	taw2 := int32(0)
	atom.AddAtom(TOTAL_ACTIVE_WORKERS, &taw2)
	tfjsd := int32(0)
	atom.AddAtom(TOTAL_FINAL_JOB_SIGNALS_DISPATCHED, &tfjsd)
	return atom
}

// add value to atom
func (a *_Atom) AddValueToAtom(id AtomicMetricName, value int32) error {
	if _, ok := a.atoms[id]; !ok {
		return errors.New("invalid id")
	}
	atom := a.atoms[id]
	atomic.AddInt32(atom, value)
	return nil
}

// increment atom
func (a *_Atom) IncrementAtom(id AtomicMetricName) error {
	if _, ok := a.atoms[id]; !ok {
		return errors.New("invalid id")
	}
	atom := a.atoms[id]
	atomic.AddInt32(atom, 1)
	return nil
}

// decrement atom
func (a *_Atom) DecrementAtom(id AtomicMetricName) error {
	if _, ok := a.atoms[id]; !ok {
		return errors.New("invalid id")
	}
	atom := a.atoms[id]
	atomic.AddInt32(atom, -1)
	return nil
}

// add atom
func (a *_Atom) AddAtom(id AtomicMetricName, value *int32) error {
	if _, ok := a.atoms[id]; ok {
		return errors.New("value already exists for key [%v]" + string(id))
	}
	a.atoms[id] = value
	return nil
}

// get atom
func (a *_Atom) GetAtom(id AtomicMetricName) (int32, error) {
	if _, ok := a.atoms[id]; !ok {
		return int32(0), errors.New("invalid id")
	}
	atom := a.atoms[id]
	result := atomic.LoadInt32(atom)
	return result, nil
}
