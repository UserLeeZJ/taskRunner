package main

import (
	"fmt"
	"os"

	"github.com/UserLeeZJ/taskrunner"
)

// FileLogger 将日志输出到文件
type FileLogger struct {
	file *os.File
}

// NewFileLogger 创建一个新的 FileLogger
func NewFileLogger(filePath string) (*FileLogger, error) {
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &FileLogger{file: f}, nil
}

// LogHandler 实现了 LogHandler 类型的方法，将日志写入文件
func (f *FileLogger) LogHandler(taskID uint, level taskrunner.LogLevel, message string) {
	logMsg := fmt.Sprintf("[%s] TaskID: %d - %s\n", level, taskID, message)
	f.file.WriteString(logMsg)
}
