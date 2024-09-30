package taskrunner

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// TaskRunner 定时任务运行器
type TaskRunner struct {
	registry *Registry
	executor *Executor
	mutexMgr *MutexManager
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// New 创建新的 TaskRunner
func New(workers int) (*TaskRunner, error) {
	// 初始化默认日志处理器
	InitializeDefaultLogHandler()

	ctx, cancel := context.WithCancel(context.Background())

	registry := NewRegistry()
	mutexMgr := NewMutexManager()
	executor := NewExecutor(ctx, registry, mutexMgr, workers)

	runner := &TaskRunner{
		registry: registry,
		executor: executor,
		mutexMgr: mutexMgr,
		ctx:      ctx,
		cancel:   cancel,
	}

	executor.Start()

	// 开启任务调度
	runner.wg.Add(1)
	go func() {
		defer runner.wg.Done()
		executor.ScheduleTasks()
	}()

	return runner, nil
}

// RegisterTask 注册一个任务
func (tr *TaskRunner) RegisterTask(name string, fn TaskFunc, intervalSecs, maxRunTimeSecs int, mutexGroup string, dependencies []string) error {
	// 先检查依赖是否已注册
	for _, dep := range dependencies {
		if _, exists := tr.registry.GetTask(dep); !exists {
			return fmt.Errorf("依赖任务 %s 未注册", dep)
		}
	}
	return tr.registry.Register(name, fn, intervalSecs, maxRunTimeSecs, mutexGroup, dependencies)
}

// SetLogHandler 允许开发者设置自定义日志处理器
func (tr *TaskRunner) SetLogHandler(handler LogHandler) {
	SetLogHandler(handler)
}

// RunTaskNow 立即运行指定任务
func (tr *TaskRunner) RunTaskNow(name string) error {
	task, exists := tr.registry.GetTask(name)
	if !exists {
		return fmt.Errorf("任务 %s 未注册", name)
	}

	// 标记任务为运行中
	tr.registry.mu.Lock()
	task.IsRunning = true
	task.LastRunTime = ptrTime(time.Now())
	tr.registry.mu.Unlock()

	// 处理互斥组
	if task.MutexGroup != "" {
		tr.mutexMgr.Enqueue(task.MutexGroup, task.Name, tr.executor.getTaskFunc(task.Name))
	} else {
		tr.executor.schedulerCh <- task.Name
	}
	return nil
}

// Stop 停止 TaskRunner
func (tr *TaskRunner) Stop() {
	tr.cancel()
	tr.executor.Stop()
	tr.wg.Wait()
}
