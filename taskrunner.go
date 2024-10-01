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
	workers  int // 新增字段
}

// New 创建新的 TaskRunner
func New(options ...Option) (*TaskRunner, error) {
	// 初始化默认日志处理器
	InitializeDefaultLogHandler()

	ctx, cancel := context.WithCancel(context.Background())

	runner := &TaskRunner{
		registry: NewRegistry(),
		mutexMgr: NewMutexManager(),
		ctx:      ctx,
		cancel:   cancel,
		workers:  1, // 默认值
	}

	// 应用所有选项
	for _, option := range options {
		option(runner)
	}

	runner.executor = NewExecutor(ctx, runner.registry, runner.mutexMgr, runner.workers)

	runner.executor.Start()

	// 开启任务调度
	runner.wg.Add(1)
	go func() {
		fmt.Println("开始调度任务")
		defer runner.wg.Done()
		runner.executor.ScheduleTasks()
	}()

	return runner, nil
}

// RegisterTask 注册一个任务
func (tr *TaskRunner) RegisterTask(name string, fn TaskFunc, intervalSecs, maxRunTimeSecs int, mutexGroup string, dependencies []string, priority int) error {
	// 先检查依赖是否已注册
	for _, dep := range dependencies {
		if _, exists := tr.registry.GetTask(dep); !exists {
			return fmt.Errorf("依赖任务 %s 未注册", dep)
		}
	}
	return tr.registry.Register(name, fn, intervalSecs, maxRunTimeSecs, mutexGroup, dependencies, priority)
}

// SetLogHandler 允许开发者设置自定义日志处理器
func (tr *TaskRunner) SetLogHandler(handler LogHandler) {
	SetLogHandler(handler)
}

// RunTaskNow 立即运行指定任务
func (tr *TaskRunner) RunTaskNow(name string, priority int) error {
	LogMessage(0, INFO, fmt.Sprintf("尝试立即执行任务: %s", name))
	task, exists := tr.registry.GetTask(name)
	if !exists {
		LogMessage(0, ERROR, fmt.Sprintf("任务 %s 不存在", name))
		return fmt.Errorf("任务 %s 未注册", name)
	}

	// 创建一个通道来等待任务执行完成
	done := make(chan struct{})
	if priority == -1 {
		priority = task.Priority // 获取任务优先级
	}
	executeTask := func() {
		defer close(done)
		LogMessage(0, INFO, fmt.Sprintf("开始执行任务: %s", name))
		tr.executor.runTask(task, tr.executor.getTaskFunc(task.Name))
		LogMessage(0, INFO, fmt.Sprintf("任务执行完成: %s", name))
	}

	if task.MutexGroup != "" {
		LogMessage(0, INFO, fmt.Sprintf("将任务 %s 加入互斥组 %s", name, task.MutexGroup))
		tr.mutexMgr.Enqueue(task.MutexGroup, task.Name, executeTask, priority) // 传递优先级
	} else {
		LogMessage(0, INFO, fmt.Sprintf("立即执行任务 %s", name))
		go executeTask()
	}

	// 等待任务执行完成或超时
	select {
	case <-done:
		LogMessage(0, INFO, fmt.Sprintf("任务 %s 执行完成", name))
	case <-time.After(time.Duration(task.MaxRunTimeSecs) * time.Second):
		LogMessage(0, ERROR, fmt.Sprintf("任务 %s 执行超时", name))
		return fmt.Errorf("任务 %s 执行超时", name)
	}

	return nil
}

// Stop 停止 TaskRunner
func (tr *TaskRunner) Stop() {
	tr.cancel()
	tr.executor.Stop()
	tr.wg.Wait()
}
