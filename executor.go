package taskrunner

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Executor 任务执行器
type Executor struct {
	registry     *Registry
	mutexManager *MutexManager
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	workers      int
	schedulerCh  chan string
}

// NewExecutor 创建新的 Executor
func NewExecutor(ctx context.Context, registry *Registry, mutexMgr *MutexManager, workers int) *Executor {
	ctx, cancel := context.WithCancel(ctx)
	return &Executor{
		registry:     registry,
		mutexManager: mutexMgr,
		ctx:          ctx,
		cancel:       cancel,
		workers:      workers,
		schedulerCh:  make(chan string),
	}
}

// Start 启动执行器
func (e *Executor) Start() {
	for i := 0; i < e.workers; i++ {
		e.wg.Add(1)
		go e.worker()
	}
}

// Stop 停止执行器
func (e *Executor) Stop() {
	e.cancel()
	close(e.schedulerCh)
	e.wg.Wait()
}

// ScheduleTasks 开始定时调度任务
func (e *Executor) ScheduleTasks() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.dispatchTasks()
		}
	}
}

// dispatchTasks 轮询所有任务，检查是否需要执行
func (e *Executor) dispatchTasks() {
	tasks := e.registry.List()
	for _, name := range tasks {
		select {
		case <-e.ctx.Done():
			return // 如果上下文被取消，立即返回
		default:
			// 继续执行
		}

		task, exists := e.registry.GetTask(name)
		if !exists || !task.IsActive || task.IsRunning {
			continue
		}

		now := time.Now()
		if task.LastRunTime == nil || now.Sub(*task.LastRunTime).Seconds() >= float64(task.IntervalSecs) {
			if e.checkDependencies(task) {
				// 标记任务为运行中
				e.registry.mu.Lock()
				task.IsRunning = true
				task.LastRunTime = ptrTime(now)
				e.registry.mu.Unlock()

				// 处理互斥组
				if task.MutexGroup != "" {
					e.mutexManager.Enqueue(task.MutexGroup, task.Name, e.getTaskFunc(task.Name))
				} else {
					select {
					case e.schedulerCh <- task.Name:
						// 成功发送任务
					case <-e.ctx.Done():
						// 如果上下文被取消，停止发送任务
						return
					}
				}
			}
		}
	}
}

// worker 工作协程，处理任务执行
func (e *Executor) worker() {
	defer e.wg.Done()
	for {
		select {
		case <-e.ctx.Done():
			return
		case name, ok := <-e.schedulerCh:
			if !ok {
				return
			}
			task, exists := e.registry.GetTask(name)
			if !exists {
				continue
			}
			fn, exists := e.registry.Get(name)
			if !exists {
				continue
			}
			e.runTask(task, fn)
		}
	}
}

// runTask 执行任务并处理日志和状态
func (e *Executor) runTask(task *ScheduledTask, fn TaskFunc) {
	defer func() {
		if rec := recover(); rec != nil {
			LogMessage(task.ID, ERROR, fmt.Sprintf("任务 %s 发生恐慌: %v", task.Name, rec))
			task.LastExecStatus = "panic"
		}
		e.registry.mu.Lock()
		task.IsRunning = false
		e.registry.mu.Unlock()
	}()

	LogMessage(task.ID, INFO, fmt.Sprintf("任务 %s 开始执行", task.Name))
	startTime := time.Now()

	// 设置任务执行超时
	done := make(chan struct{})
	go func() {
		fn()
		close(done)
	}()

	select {
	case <-done:
		// 任务完成
		endTime := time.Now()
		duration := endTime.Sub(startTime)
		LogMessage(task.ID, INFO, fmt.Sprintf("任务 %s 执行完成，耗时 %s", task.Name, duration))
		task.LastExecStatus = "success"
	case <-time.After(time.Duration(task.MaxRunTimeSecs) * time.Second):
		// 任务超时
		LogMessage(task.ID, ERROR, fmt.Sprintf("任务 %s 执行超时(%d秒)", task.Name, task.MaxRunTimeSecs))
		task.LastExecStatus = "timeout"
	}

	// 处理互斥组队列
	if task.MutexGroup != "" {
		if entry, ok := e.mutexManager.Dequeue(task.MutexGroup); ok {
			taskNext, exists := e.registry.GetTask(entry.taskName)
			if exists {
				fnNext, exists := e.registry.Get(entry.taskName)
				if exists {
					e.runTask(taskNext, fnNext)
				}
			}
		} else {
			e.mutexManager.Finish(task.MutexGroup)
		}
	}
}

// checkDependencies 检查任务的依赖是否满足
func (e *Executor) checkDependencies(task *ScheduledTask) bool {
	if len(task.Dependencies) == 0 {
		return true
	}
	for _, dep := range task.Dependencies {
		depTask, exists := e.registry.GetTask(dep)
		if !exists || depTask.LastExecStatus != "success" {
			return false
		}
	}
	return true
}

// getTaskFunc 获取任务函数
func (e *Executor) getTaskFunc(name string) TaskFunc {
	fn, exists := e.registry.Get(name)
	if !exists {
		return func() {}
	}
	return fn
}

func ptrTime(t time.Time) *time.Time {
	return &t
}
