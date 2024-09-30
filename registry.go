package taskrunner

import (
	"fmt"
	"sync"
	"time"
)

// TaskFunc 任务函数类型
type TaskFunc func()

// ScheduledTask 定时任务结构体
type ScheduledTask struct {
	ID             uint
	Name           string
	IntervalSecs   int
	MaxRunTimeSecs int
	IsActive       bool
	LastRunTime    *time.Time
	LastExecStatus string
	IsRunning      bool
	MutexGroup     string
	Dependencies   []string
}

// Registry 任务注册表
type Registry struct {
	tasks    map[string]*ScheduledTask
	mu       sync.RWMutex
	handlers map[string]TaskFunc
}

// NewRegistry 创建新的任务注册表
func NewRegistry() *Registry {
	return &Registry{
		tasks:    make(map[string]*ScheduledTask),
		handlers: make(map[string]TaskFunc),
	}
}

// Register 注册一个任务
func (r *Registry) Register(name string, fn TaskFunc, intervalSecs, maxRunTimeSecs int, mutexGroup string, dependencies []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.tasks[name]; exists {
		return fmt.Errorf("任务 %s 已存在", name)
	}
	r.tasks[name] = &ScheduledTask{
		ID:             generateTaskID(),
		Name:           name,
		IntervalSecs:   intervalSecs,
		MaxRunTimeSecs: maxRunTimeSecs,
		IsActive:       true,
		MutexGroup:     mutexGroup,
		Dependencies:   dependencies,
	}
	r.handlers[name] = fn
	return nil
}

// Get 获取任务函数
func (r *Registry) Get(name string) (TaskFunc, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fn, exists := r.handlers[name]
	return fn, exists
}

// List 返回所有注册的任务名称
func (r *Registry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.tasks))
	for name := range r.tasks {
		names = append(names, name)
	}
	return names
}

// GetTask 获取任务详情
func (r *Registry) GetTask(name string) (*ScheduledTask, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	task, exists := r.tasks[name]
	return task, exists
}

// validateDependencies 检查任务的依赖是否已注册
func (r *Registry) validateDependencies() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, task := range r.tasks {
		for _, dep := range task.Dependencies {
			if _, exists := r.tasks[dep]; !exists {
				return fmt.Errorf("任务 %s 的依赖任务 %s 未注册", task.Name, dep)
			}
		}
	}
	return nil
}

// generateTaskID 生成唯一任务ID（简单实现）
var (
	lastTaskID uint = 0
	idMu       sync.Mutex
)

func generateTaskID() uint {
	idMu.Lock()
	defer idMu.Unlock()
	lastTaskID++
	return lastTaskID
}
