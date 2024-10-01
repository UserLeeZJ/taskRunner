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
	runner, err := taskrunner.New(taskrunner.WithWorkers(2))
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
		t.Logf("日志: %s", logEntry) // 实时查看日志
	}
	runner.SetLogHandler(memoryLogger)

	// 测试任务执行计数
	taskCounters := make(map[string]int)
	taskMu := sync.Mutex{}

	// 注册任务并设置优先级
	err = runner.RegisterTask("HighPriorityTask", func() {
		taskMu.Lock()
		taskCounters["HighPriorityTask"]++
		taskMu.Unlock()
		t.Log("HighPriorityTask 执行中...")
		time.Sleep(1 * time.Second)
		t.Log("HighPriorityTask 执行完成")
	}, 10, 5, "", []string{}, 10) // 高优先级

	if err != nil {
		t.Fatalf("注册 HighPriorityTask 失败: %v", err)
	}

	err = runner.RegisterTask("LowPriorityTask", func() {
		taskMu.Lock()
		taskCounters["LowPriorityTask"]++
		taskMu.Unlock()
		t.Log("LowPriorityTask 执行中...")
		time.Sleep(1 * time.Second)
		t.Log("LowPriorityTask 执行完成")
	}, 10, 5, "", []string{}, 1) // 低优先级

	if err != nil {
		t.Fatalf("注册 LowPriorityTask 失败: %v", err)
	}

	// 让任务有机会执行
	t.Log("等待任务执行...")
	time.Sleep(3 * time.Second)

	// 检查任务执行次数
	taskMu.Lock()
	highCount := taskCounters["HighPriorityTask"]
	lowCount := taskCounters["LowPriorityTask"]
	taskMu.Unlock()

	if highCount == 0 {
		t.Errorf("HighPriorityTask 未被执行")
	} else {
		t.Logf("HighPriorityTask 执行次数: %d", highCount)
	}

	if lowCount == 0 {
		t.Errorf("LowPriorityTask 未被执行")
	} else {
		t.Logf("LowPriorityTask 执行次数: %d", lowCount)
	}

	// 测试立即执行高优先级任务
	t.Log("测试立即执行高优先级任务...")
	err = runner.RunTaskNow("HighPriorityTask", 10) // 传递优先级
	if err != nil {
		t.Errorf("立即执行 HighPriorityTask 失败: %v", err)
	}

	time.Sleep(2 * time.Second)

	// 检查日志
	logMu.Lock()
	allLogs := logs
	logMu.Unlock()

	t.Log("检查日志...")
	for _, log := range allLogs {
		t.Logf("日志: %s", log)
	}

	// 检查是否有 HighPriorityTask 的执行完成日志
	foundExecutionLog := false
	for _, log := range allLogs {
		if strings.Contains(log, "HighPriorityTask") && strings.Contains(log, "执行完成") {
			foundExecutionLog = true
			break
		}
	}

	if !foundExecutionLog {
		t.Errorf("未找到 HighPriorityTask 的执行完成日志")
	} else {
		t.Log("成功找到 HighPriorityTask 的执行完成日志")
	}

	// 检查 HighPriorityTask 的执行次数是否增加
	taskMu.Lock()
	newHighCount := taskCounters["HighPriorityTask"]
	taskMu.Unlock()

	if newHighCount < highCount+1 {
		t.Errorf("HighPriorityTask 执行次数未增加")
	} else {
		t.Logf("HighPriorityTask 执行次数: %d", newHighCount)
	}
}
