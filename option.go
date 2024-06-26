package batchworker

import "time"

const (
	DefaultThreadNum       = 1
	DefaultChanSize        = 100
	DefaultMaxBatchNum     = 50
	DefaultMaxWaitInterval = 2 * time.Second
)

// OptionFunc type
type OptionFunc func(opts *Options)

// Options 消费者自定义参数
type Options struct {
	ThreadNum       int
	ChanSize        int
	MaxBatchNum     int
	MaxWaitInterval time.Duration
	Handler         func(interface{})
}

// WithThreads int 消费组协程数
func WithThreads(num int) OptionFunc {
	return func(opts *Options) {
		opts.ThreadNum = num
	}
}

// WithChanSize 缓冲区大小
func WithChanSize(num int) OptionFunc {
	return func(opts *Options) {
		opts.ChanSize = num
	}
}

// WithTaskHandler 上报处理方法
func WithTaskHandler(pf func(interface{})) OptionFunc {
	return func(opts *Options) {
		opts.Handler = pf
	}
}

// WithMaxBatchNum 最大批处理数量
func WithMaxBatchNum(maxBatchNum int) OptionFunc {
	return func(opts *Options) {
		opts.MaxBatchNum = maxBatchNum
	}
}

// WithMaxWaitInterval 最大批处理等待时间
func WithMaxWaitInterval(maxWaitInterval time.Duration) OptionFunc {
	return func(opts *Options) {
		opts.MaxWaitInterval = maxWaitInterval
	}
}

func LoadOptions(options ...OptionFunc) *Options {
	opts := &Options{
		ThreadNum:       DefaultThreadNum,
		ChanSize:        DefaultChanSize,
		MaxBatchNum:     DefaultMaxBatchNum,
		MaxWaitInterval: DefaultMaxWaitInterval,
	}

	for _, option := range options {
		option(opts)
	}

	return opts
}
