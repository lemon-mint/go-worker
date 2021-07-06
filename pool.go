package goworker

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/lemon-mint/unlock"
)

type Pool struct {
	maxWorkers    int
	taskQueueSize int

	workers int64
	idle    int64

	tidCounter uint64
	widCounter uint64

	taskQueue      *unlock.RingBuffer
	workerDataPool sync.Pool

	HandlerFunc func(Task)
}

type Task struct {
	WorkerID uint64
	TaskID   uint64

	Data unsafe.Pointer
}

type WorkerCommand byte

const (
	Stop = WorkerCommand(iota)
	Pause
	RunHandler
)

type workerData struct {
	CommandType WorkerCommand

	Data unsafe.Pointer
}

func NewPool(maxWorkers int, taskQueueSize int) *Pool {
	pool := &Pool{
		maxWorkers:    maxWorkers,
		taskQueueSize: taskQueueSize,
		taskQueue:     unlock.NewRingBuffer(taskQueueSize),
		// Performance optimization by sync.Pool
		workerDataPool: sync.Pool{New: func() interface{} {
			return new(workerData)
		}},
	}
	return pool
}

func (p *Pool) RunTask(data unsafe.Pointer) {
	if atomic.LoadInt64(&p.idle) == 0 && int(atomic.LoadInt64(&p.workers)) < p.maxWorkers {
		go p.worker()
	}
	wd := p.workerDataPool.Get().(*workerData)
	wd.CommandType = RunHandler
	wd.Data = data
	p.taskQueue.EnQueue(unsafe.Pointer(wd))
}

// StopWorker stops the one worker
func (p *Pool) StopWorker() {
	wd := p.workerDataPool.Get().(*workerData)
	wd.CommandType = Stop
	wd.Data = nil
	p.taskQueue.EnQueue(unsafe.Pointer(wd))
}

func (p *Pool) worker() {
	WorkerID := atomic.AddUint64(&p.widCounter, 1)
	atomic.AddInt64(&p.workers, 1)
	atomic.AddInt64(&p.idle, 1)
	defer atomic.AddInt64(&p.workers, -1)
	for {
		wd := (*workerData)(p.taskQueue.DeQueue())
		defer p.workerDataPool.Put(wd)
		atomic.AddInt64(&p.idle, -1)
		TaskID := atomic.AddUint64(&p.tidCounter, 1)
		switch wd.CommandType {
		case RunHandler:
			p.HandlerFunc(Task{
				WorkerID: WorkerID,
				TaskID:   TaskID,
				Data:     wd.Data,
			})
		case Stop:
			return
		}
		atomic.AddInt64(&p.idle, 1)
	}
}
