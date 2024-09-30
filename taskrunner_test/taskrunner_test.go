package taskrunner_test

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/UserLeeZJ/taskrunner"
)

func TestTaskRunner(t *testing.T) {
	// 创建 TaskRunner
	runner, err := taskrunner.New(2)
	if err != nil {
		t.Fatalf("创建 TaskRunner 失败: %v", err)
	}
	defer runner.Stop()

	// 设置自定义日志处理器（内存日志）
	var logs []string
	logMu := sync.Mutex{}
	memoryLogger := func(taskID uint, level taskrunner.LogLevel, message string) {
		logMu.Lock()
		defer logMu.Unlock()
		logEntry := fmt.Sprintf("[%s] TaskID: %d - %s", level, taskID, message)
		logs = append(logs, logEntry)
	}
	runner.SetLogHandler(memoryLogger)

	// 注册一个测试任务
	testTask := func() {
		// 模拟任务执行
		time.Sleep(100 * time.Millisecond)
	}
	err = runner.RegisterTask("TestTask", testTask, 1, 2, "", []string{})
	if err != nil {
		t.Fatalf("注册 TestTask 失败: %v", err)
	}

	// 让任务有机会执行
	time.Sleep(3 * time.Second)

	// 检查日志
	logMu.Lock()
	defer logMu.Unlock()
	if len(logs) == 0 {
		t.Errorf("预期有日志记录，实际无记录")
	}

	// 检查最后一条日志是否为成功执行
	lastLog := logs[len(logs)-1]
	if !strings.Contains(lastLog, "INFO") || !strings.Contains(lastLog, "执行完成") {
		t.Errorf("预期最后一条日志为成功执行，实际为: %s", lastLog)
	}
}
