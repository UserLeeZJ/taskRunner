package taskrunner

import (
	"fmt"
	"sync"
	"time"
)

// LogLevel 日志级别
type LogLevel string

const (
	INFO  LogLevel = "INFO"
	WARN  LogLevel = "WARN"
	ERROR LogLevel = "ERROR"
)

// LogHandler 日志处理函数类型
type LogHandler func(taskID uint, level LogLevel, message string)

// 全局日志处理器
var (
	logHandler     LogHandler
	logHandlerMu   sync.RWMutex
	defaultHandler LogHandler
)

// SetLogHandler 注册一个自定义日志处理器
func SetLogHandler(handler LogHandler) {
	logHandlerMu.Lock()
	defer logHandlerMu.Unlock()
	logHandler = handler
}

// GetLogHandler 获取当前的日志处理器
func GetLogHandler() LogHandler {
	logHandlerMu.RLock()
	defer logHandlerMu.RUnlock()
	if logHandler != nil {
		return logHandler
	}
	return defaultHandler
}

// LogMessage 记录日志
func LogMessage(taskID uint, level LogLevel, message string) {
	GetLogHandler()(taskID, level, message)
}

// InitializeDefaultLogHandler 初始化默认日志处理器（输出到控制台）
func InitializeDefaultLogHandler() {
	defaultHandler = func(taskID uint, level LogLevel, message string) {
		timestamp := time.Now().Format(time.RFC3339)
		fmt.Printf("[%s] [%s] TaskID: %d - %s\n", timestamp, level, taskID, message)
	}
}

// NoOpLogHandler 空操作日志处理器，不记录日志
func NoOpLogHandler() LogHandler {
	return func(taskID uint, level LogLevel, message string) {
		// 不执行任何操作
	}
}
