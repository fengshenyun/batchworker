package batchworker

import (
	"github.com/fengshenyun/logrec"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type Message struct {
	I int
	J string
}

func handlerTest(data interface{}) {}

func TestNoLog(t *testing.T) {
	opts := []OptionFunc{
		WithTaskHandler(handlerTest),
	}

	w, err := New(opts...)
	assert.Equal(t, nil, err)

	Info(w.log, "abc")
}

func TestWithLog(t *testing.T) {
	opts := []OptionFunc{
		WithTaskHandler(handlerTest),
	}

	logOpts := []logrec.Option{
		logrec.WithFileName("log/single"),
		logrec.WithLogLevel(logrec.LogLevelTrace),
	}

	logger := logrec.NewSingleLoggerWithOptions(logOpts...)

	w, err := NewWithLogger(logger, opts...)
	assert.Equal(t, nil, err)

	Info(w.log, "abcdef")
	time.Sleep(1 * time.Second)
}
