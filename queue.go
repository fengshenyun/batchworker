package batchworker

import (
	"context"
	"github.com/pkg/errors"
)

var (
	ErrNoData      = errors.New("no data")
	ErrQueueClosed = errors.New("queue closed")
)

type Queue struct {
	queue  chan interface{}
	ctx    context.Context
	cancel context.CancelFunc
	size   int
}

func NewQueue(size int) *Queue {
	ctx, cancel := context.WithCancel(context.Background())
	if size <= 0 {
		size = 1024
	}
	return &Queue{
		queue:  make(chan interface{}, size),
		ctx:    ctx,
		cancel: cancel,
		size:   size,
	}
}

func (q *Queue) Get(ctx context.Context) (interface{}, error) {
	select {
	case <-q.ctx.Done():
		return nil, ErrQueueClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	case data := <-q.queue:
		return data, nil
	}
}

func (q *Queue) GetWithNoBlock(ctx context.Context) (interface{}, error) {
	select {
	case <-q.ctx.Done():
		return nil, ErrQueueClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	case data := <-q.queue:
		return data, nil
	default:
		return nil, ErrNoData
	}
}

func (q *Queue) Put(ctx context.Context, data interface{}) error {
	select {
	case <-q.ctx.Done():
		return ErrQueueClosed
	case <-ctx.Done():
		return ctx.Err()
	case q.queue <- data:
		return nil
	}
}

func (q *Queue) Close() error {
	q.cancel()
	return nil
}

func (q *Queue) Len() int {
	return len(q.queue)
}

func (q *Queue) Remain() int {
	return q.size - len(q.queue)
}
