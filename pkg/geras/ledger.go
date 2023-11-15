package geras

import (
	"time"

	"github.com/outofoffice3/geras-sandbox-pro/internal/atoms"
)

// ledger interface for collecting job execution metadata
type ledger interface {
	// add entry to ledger
	addEntry(_LedgerEntryInput) error
	// get ledger entry by id
	getEntry(id string) (*_LedgerEntry, bool)
	// get job stats for all jobs in ledger
}

type _LedgerEntryInput struct {
	id                       string
	jobName                  string
	timeAddedToBufferedQueue time.Time
	status                   _JobStatus
}

// struct for storing ledger entries
type _LedgerEntry struct {
	jobId       string       // job that was processed
	workerId    int          // worker that processed the job
	atomicState atoms.Atoms  // atomic state
	jobMetadata _JobMetadata // job metadata
}

type _JobMetadata struct {
	timeAddedToBufferedQueue time.Time  // time job added to buffered queue
	timePickedUpByDispatcher time.Time  // time job picked up by dispatcher
	timeDispatched           time.Time  // time job dispatched
	timeStarted              time.Time  // time job started
	timeCompleted            time.Time  // time job complete
	status                   _JobStatus // job status
	errMsg                   string     // error message if any
}

// type implementation of ledger interface
type _Ledger struct {
	entries map[string]_LedgerEntry // map for storing ledger entries
}

// input for creating a ledger
type ledgerInput struct {
}

// create a new ledger
func newLedger(input ledgerInput) ledger {
	l := &_Ledger{
		entries: make(map[string]_LedgerEntry), // initialize map to hold entries
	}
	return l
}

type _JobMetadataInput struct {
	status                   _JobStatus
	timeAddedToBufferedQueue time.Time
}

// add entry to ledger
func (l *_Ledger) addEntry(input _LedgerEntryInput) error {
	// check if entry already exists in ledger
	if _, ok := l.entries[input.id]; ok {
		return newError("entry already exists with the same key")
	}
	// create and initialize atomic state
	atomicState := atoms.NewAtoms()
	tc := int32(0)
	atomicState.AddAtom(atoms.JOB_THROTTLE_COUNT, &tc)
	td := int32(0)
	atomicState.AddAtom(atoms.JOB_THROTTLE_DURATION, &td)
	jd := int32(0)
	atomicState.AddAtom(atoms.JOB_DURATION, &jd)
	dbd := int32(0)
	atomicState.AddAtom(atoms.JOB_DISPATCHER_BLOCKED_DURATION, &dbd)

	// create entry
	entry := _LedgerEntry{
		jobId:       input.id,
		atomicState: atomicState,
		jobMetadata: _JobMetadata{
			status:                   input.status,
			timeAddedToBufferedQueue: input.timeAddedToBufferedQueue,
		},
	}
	// add entry to map based on its id
	l.entries[entry.jobId] = entry
	return nil
}

// get ledger entry by id
func (l *_Ledger) getEntry(id string) (*_LedgerEntry, bool) {
	// check if entry exists in ledger
	if _, ok := l.entries[id]; !ok {
		return nil, false // entry does not exist with given key
	}
	entry := l.entries[id] // get entry from map
	return &entry, true    // return pointer to entry for modification
}
