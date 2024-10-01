package taskrunner

// Option 定义一个函数类型，用于设置 TaskRunner 的选项
type Option func(*TaskRunner)

// WithWorkers 设置工作协程数
func WithWorkers(workers int) Option {
	return func(tr *TaskRunner) {
		tr.workers = workers
	}
}

// WithLogHandler 设置自定义日志处理器
func WithLogHandler(handler LogHandler) Option {
	return func(tr *TaskRunner) {
		SetLogHandler(handler)
	}
}
