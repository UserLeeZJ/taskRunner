package taskrunner

import (
	"sync"
)

// MutexManager 管理互斥组的锁和队列
type MutexManager struct {
	groups map[string]*GroupQueue
	mu     sync.Mutex
}

// GroupQueue 表示一个互斥组的任务队列
type GroupQueue struct {
	queue   []TaskExecutionEntry
	running bool
	mu      sync.Mutex
}

// TaskExecutionEntry 任务执行条目
type TaskExecutionEntry struct {
	taskName string
	execute  TaskFunc
}

// NewMutexManager 创建一个新的 MutexManager
func NewMutexManager() *MutexManager {
	return &MutexManager{
		groups: make(map[string]*GroupQueue),
	}
}

// Enqueue 将任务添加到互斥组队列
func (m *MutexManager) Enqueue(group string, taskName string, execute TaskFunc) {
	m.mu.Lock()
	groupQueue, exists := m.groups[group]
	if !exists {
		groupQueue = &GroupQueue{}
		m.groups[group] = groupQueue
	}
	m.mu.Unlock()

	groupQueue.mu.Lock()
	groupQueue.queue = append(groupQueue.queue, TaskExecutionEntry{
		taskName: taskName,
		execute:  execute,
	})
	groupQueue.mu.Unlock()
}

// Dequeue 获取下一个任务并标记为正在运行
func (m *MutexManager) Dequeue(group string) (TaskExecutionEntry, bool) {
	m.mu.Lock()
	groupQueue, exists := m.groups[group]
	m.mu.Unlock()
	if !exists {
		return TaskExecutionEntry{}, false
	}

	groupQueue.mu.Lock()
	defer groupQueue.mu.Unlock()
	if len(groupQueue.queue) == 0 || groupQueue.running {
		return TaskExecutionEntry{}, false
	}

	task := groupQueue.queue[0]
	groupQueue.queue = groupQueue.queue[1:]
	groupQueue.running = true
	return task, true
}

// Finish 设置互斥组的运行状态为完成
func (m *MutexManager) Finish(group string) {
	m.mu.Lock()
	groupQueue, exists := m.groups[group]
	m.mu.Unlock()
	if !exists {
		return
	}

	groupQueue.mu.Lock()
	defer groupQueue.mu.Unlock()
	groupQueue.running = false
}
