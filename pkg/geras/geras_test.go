package geras

import (
	"fmt"
	_ "net/http/pprof"
	"runtime"
	"testing"

	"github.com/outofoffice3/common/logger"
	"github.com/stretchr/testify/assert"
)

type testJob struct {
	id   string
	name string
}

func (tj *testJob) Do() error {
	fmt.Printf("TEST JOB DONE [%v] SUCCESSFULLY", tj.id)
	return nil
}

func (tj *testJob) SetJobName(name string) {
	tj.name = name
}

func (tj *testJob) GetJobName() string {
	return tj.name
}

func (tj *testJob) GetJobId() string {
	return tj.id
}

func (tj *testJob) SetJobId(id string) error {
	tj.id = id
	return nil
}

type failJob struct {
	id   string
	name string
}

func (tj *failJob) Do() error {
	fmt.Printf("TEST JOB FAILED [%v] SUCCESSFULLY", tj.id)
	return newError("job [%v] failed" + tj.id)
}

func (tj *failJob) SetJobName(name string) {
	tj.name = name
}

func (tj *failJob) GetJobName() string {
	return tj.name
}

func (tj *failJob) GetJobId() string {
	return tj.id
}

func (tj *failJob) SetJobId(id string) error {
	tj.id = id
	return nil
}

func TestGQ(t *testing.T) {
	a := assert.New(t)
	// initialize GQ package
	Init(logger.LogLevelDebug)
	numWorker := 3
	queueSize := 10
	rateLimit := 10
	rateTimeWindow := 1

	// Create a new GQ
	gqConfig := NewGerasInput{
		NumWorkers:     numWorker,
		QueueSize:      queueSize,
		RateLimit:      rateLimit,
		RateTimeWindow: rateTimeWindow,
	}

	q, err := NewGeras(gqConfig)
	q.SetErrorHandler(func(err error) {
		fmt.Printf("ERROR: %v", err)
	})
	a.NoError(err)
	_Geras, ok := q.(*_Geras)
	a.True(ok)                    // should be type *_Geras
	a.NotNil(_Geras.errorHandler) // error handler should be set

	// TEST GQ INITIALIZATION
	a.Equal(int32(3), q.GetTotalWorkerCount())
	a.Equal(int32(0), q.GetFailedJobsCount())
	a.Equal(int32(0), q.GetTotalJobsDispatched())
	a.Equal(int32(0), q.GetTotalJobsThrottled())
	a.Equal(queueSize, q.Cap())
	a.Equal(0, q.Len())

	tj := &testJob{}
	testJobId := "test-job-1"
	tj.SetJobId(testJobId)
	testJobName := "test-job-name"
	tj.SetJobName(testJobName)
	err = q.AddJob(tj)
	a.NoError(err)

	fj := &failJob{}
	failJobId := "fail-job-1"
	fj.SetJobId(failJobId)
	failJobName := "fail-job-name"
	fj.SetJobName(failJobName)
	err = q.AddJob(fj)
	a.NoError(err)

	// TEST ADD JOB
	a.Equal(int32(3), q.GetTotalWorkerCount())    // total should be 3
	a.Equal(int32(0), q.GetFailedJobsCount())     // should be 0
	a.Equal(int32(0), q.GetTotalJobsDispatched()) // should be 0
	a.Equal(2, q.Len())

	err = q.Start() // start GQ
	a.NoError(err)  // should be open for jobs
	err = q.Stop()  // stop queue
	a.NoError(err)
	a.Equal(int32(1), q.GetFailedJobsCount())     // should be 1
	a.Equal(int32(3), q.GetTotalWorkerCount())    // should be 0
	a.Equal(int32(2), q.GetTotalJobsDispatched()) // should be 2
	a.Equal(0, q.Len())

	runtime.GC()
	a.Equal(2, runtime.NumGoroutine())

	throttledGQConfig := NewGerasInput{
		NumWorkers:     4,
		QueueSize:      10,
		RateLimit:      1,
		RateTimeWindow: 2,
	}
	throttledGQ, err := NewGeras(throttledGQConfig)
	a.NoError(err)
	a.NotNil(throttledGQ)
	err = throttledGQ.Start()
	a.NoError(err)

	// add 5 jobs in a loop
	for i := 0; i < 5; i++ {
		err := throttledGQ.AddJob(&testJob{
			id:   fmt.Sprintf("test-job-%v", i),
			name: fmt.Sprintf("test-job-%v", i),
		})
		a.NoError(err)
	}
	err = throttledGQ.Stop()
	a.NoError(err)
	a.Equal(int32(0), throttledGQ.GetFailedJobsCount())
	a.Equal(int32(5), throttledGQ.GetTotalJobsDispatched())
	a.Equal(int32(4), throttledGQ.GetTotalJobsThrottled())

	runtime.GC()
	a.Equal(2, runtime.NumGoroutine())

	config := NewGerasInput{
		NumWorkers:     0,
		RateLimit:      10,
		RateTimeWindow: 1,
		QueueSize:      10,
	}
	errorGeras, err := NewGeras(config)
	a.Nil(errorGeras, "should be nil")
	a.Error(err, "should return an error")

	runtime.GC()
	a.Equal(2, runtime.NumGoroutine())
}
