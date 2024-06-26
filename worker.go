package batchworker

import (
	"context"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/pkg/errors"
)

type Worker struct {
	*Options
	queue *Queue
	pool  *ants.PoolWithFunc
	log   Logger
}

func New(opts ...OptionFunc) (*Worker, error) {
	return NewWithLogger(nil, opts...)
}

func NewWithLogger(log Logger, opts ...OptionFunc) (*Worker, error) {
	o := LoadOptions(opts...)
	if o.Handler == nil {
		return nil, errors.New("no task handler")
	}

	p, err := ants.NewPoolWithFunc(o.ThreadNum, o.Handler)
	if err != nil {
		return nil, errors.Wrapf(err, "init pool failed")
	}

	r := &Worker{
		Options: o,
		pool:    p,
		queue:   NewQueue(o.ChanSize),
	}

	if log != nil {
		r.log = log
	}
	return r, nil
}

func (r *Worker) Put(ctx context.Context, data interface{}) {
	if err := r.queue.Put(ctx, data); err != nil {
		Error(r.log, "put task data failed, err:%v", err)
	} else {
		Debug(r.log, "add task success, current queue len:%v", r.queue.Len())
	}
}

func (r *Worker) Run() {
	for {
		data, err := r.queue.Get(context.Background())
		if err != nil {
			Error(r.log, "get task data failed, err:%v", err)
			break
		}

		Debug(r.log, "task data: %v", data)

		if err = r.pool.Invoke(data); err != nil {
			Error(r.log, "pool invoke failed, err:%v", err)
		} else {
			Debug(r.log, "process task success, current queue len:%v", r.queue.Len())
		}
	}

	r.pool.Release()
	Debug(r.log, "terminate report")
}

func (r *Worker) RunWithContext() {
	var (
		maxBatchNum     = r.MaxBatchNum
		maxWaitInterval = r.MaxWaitInterval
	)

	go func() {
		for {
			Error(r.log, "tasks in queue:%v", r.queue.Len())
			time.Sleep(3 * time.Second)
		}
	}()

	for {
		start := time.Now()
		done := make(chan bool)
		messages := make([]interface{}, 0)

		ctx, cancel := context.WithTimeout(context.Background(), maxWaitInterval)
		go func() {
			for i := 0; i < maxBatchNum; i++ {
				if isDone(ctx) > 0 {
					break
				}

				msg, err := r.queue.Get(ctx)
				if err != nil {
					Error(r.log, "get task data failed, err:%v", err)
				} else {
					messages = append(messages, msg)
				}
			}

			if isDone(ctx) == 0 {
				done <- true
				Error(r.log, "task done, messages size:%v", len(messages))
			} else {
				Error(r.log, "timeout, messages size:%v", len(messages))
			}
		}()

		select {
		case <-done:
			Error(r.log, "task done.")
		case <-ctx.Done():
			Error(r.log, "timeout.")
		}

		cancel()

		if len(messages) == 0 {
			time.Sleep(1 * time.Second)
			Error(r.log, "no data, sleep a wheel")
			continue
		}

		Debug(r.log, "task data size: %v", len(messages))

		if err := r.pool.Invoke(messages); err != nil {
			Error(r.log, "pool invoke failed, err:%v", err)
		} else {
			Debug(r.log, "process task success, current queue len:%v", r.queue.Len())
		}

		// 降频
		elapsed := time.Since(start).Milliseconds()
		r.log.Error("task elapsed %vms", elapsed)
	}
}

func isDone(ctx context.Context) int {
	select {
	case <-ctx.Done():
		if ctx.Err() == context.Canceled {
			return 1
		}
		if ctx.Err() == context.DeadlineExceeded {
			return 2
		}
		return 0
	default:
	}

	return 0
}
