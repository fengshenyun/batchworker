package batchworker

type Logger interface {
	Info(format string, v ...interface{})
	Debug(format string, v ...interface{})
	Error(format string, v ...interface{})
}

func Info(log Logger, format string, v ...interface{}) {
	if log != nil {
		log.Info(format, v...)
	}
}

func Debug(log Logger, format string, v ...interface{}) {
	if log != nil {
		log.Debug(format, v...)
	}
}

func Error(log Logger, format string, v ...interface{}) {
	if log != nil {
		log.Error(format, v...)
	}
}
