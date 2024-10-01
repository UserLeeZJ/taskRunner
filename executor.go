package taskrunner

import (
	"container/heap"
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
	pq           PriorityQueue
	pqMutex      sync.Mutex
	pqCond       *sync.Cond
	schedulerCh  chan string // 保持现有通道以兼容互斥组任务
}

// NewExecutor 创建新的 Executor
func NewExecutor(ctx context.Context, registry *Registry, mutexMgr *MutexManager, workers int) *Executor {
	ctx, cancel := context.WithCancel(ctx)
	pq := make(PriorityQueue, 0)
	executor := &Executor{
		registry:     registry,
		mutexManager: mutexMgr,
		ctx:          ctx,
		cancel:       cancel,
		workers:      workers,
		pq:           pq,
		schedulerCh:  make(chan string),
	}
	executor.pqCond = sync.NewCond(&executor.pqMutex)
	heap.Init(&executor.pq)
	return executor
}

// Start 启动执行器
func (e *Executor) Start() {
	for i := 0; i < e.workers; i++ {
		e.wg.Add(1)
		go e.worker()
	}
	LogMessage(0, INFO, fmt.Sprintf("启动了 %d 个 worker", e.workers))
}

// Stop 停止执行器
func (e *Executor) Stop() {
	e.cancel()
	close(e.schedulerCh)
	e.wg.Wait()
}

// ScheduleTasks 开始定时调度任务
func (e *Executor) ScheduleTasks() {
	LogMessage(0, INFO, "开始调度任务")
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-e.ctx.Done():
			LogMessage(0, INFO, "停止调度任务")
			return
		case <-ticker.C:
			e.dispatchTasks()
		}
	}
}

// dispatchTasks 轮询所有任务，检查是否需要执行
func (e *Executor) dispatchTasks() {
	tasks := e.registry.List()
	LogMessage(0, INFO, fmt.Sprintf("开始调度 %d 个任务", len(tasks)))
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
				LogMessage(0, INFO, fmt.Sprintf("调度任务: %s, 互斥组: %s, 优先级: %d", name, task.MutexGroup, task.Priority))
				// 标记任务为运行中
				e.registry.mu.Lock()
				task.IsRunning = true
				task.LastRunTime = ptrTime(now)
				e.registry.mu.Unlock()

				if task.MutexGroup != "" {
					e.mutexManager.Enqueue(task.MutexGroup, task.Name, func() {
						e.runTask(task, e.getTaskFunc(task.Name))
					}, task.Priority) // 传递优先级
				} else {
					// 将任务添加到优先级队列
					e.pqMutex.Lock()
					heap.Push(&e.pq, &TaskItem{
						name:     name,
						priority: task.Priority,
					})
					e.pqCond.Signal() // 唤醒等待的 worker
					e.pqMutex.Unlock()
					LogMessage(0, INFO, fmt.Sprintf("任务 %s 已添加到优先级队列", name))
				}
			}
		}
	}
}

// worker 工作协程，处理任务执行
func (e *Executor) worker() {
	defer e.wg.Done()
	LogMessage(0, INFO, "worker 开始工作")
	for {
		e.pqMutex.Lock()
		for e.pq.Len() == 0 {
			e.pqCond.Wait()
			select {
			case <-e.ctx.Done():
				LogMessage(0, INFO, "worker 停止工作")
				e.pqMutex.Unlock()
				return
			default:
			}
		}
		item := heap.Pop(&e.pq).(*TaskItem)
		e.pqMutex.Unlock()

		LogMessage(0, INFO, fmt.Sprintf("worker 接收到任务: %s", item.name))
		task, exists := e.registry.GetTask(item.name)
		if !exists {
			LogMessage(0, ERROR, fmt.Sprintf("任务不存在: %s", item.name))
			continue
		}
		fn, exists := e.registry.Get(item.name)
		if !exists {
			LogMessage(0, ERROR, fmt.Sprintf("任务函数不存在: %s", item.name))
			continue
		}
		LogMessage(0, INFO, fmt.Sprintf("执行任务函数: %s", item.name))
		e.runTask(task, fn)
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
		LogMessage(task.ID, INFO, fmt.Sprintf("任务 %s 执行结束", task.Name))
	}()

	LogMessage(task.ID, INFO, fmt.Sprintf("开始执行任务: %s", task.Name))
	startTime := time.Now()

	done := make(chan struct{})
	go func() {
		fn()
		close(done)
	}()

	select {
	case <-done:
		endTime := time.Now()
		duration := endTime.Sub(startTime)
		LogMessage(task.ID, INFO, fmt.Sprintf("任务 %s 执行完成，耗时 %s", task.Name, duration))
		task.LastExecStatus = "success"
	case <-time.After(time.Duration(task.MaxRunTimeSecs) * time.Second):
		LogMessage(task.ID, ERROR, fmt.Sprintf("任务 %s 执行超时(%d秒)", task.Name, task.MaxRunTimeSecs))
		task.LastExecStatus = "timeout"
	}

	if task.MutexGroup != "" {
		e.mutexManager.Finish(task.MutexGroup)
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

// TaskItem 包含任务名称和优先级
type TaskItem struct {
	name     string
	priority int
	index    int
}

// PriorityQueue 实现 heap.Interface
type PriorityQueue []*TaskItem

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// 数值越大优先级越高
	return pq[i].priority > pq[j].priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*TaskItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // 避免内存泄漏
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// Peek 返回队列中优先级最高的任务
func (pq PriorityQueue) Peek() *TaskItem {
	if len(pq) == 0 {
		return nil
	}
	return pq[0]
}
