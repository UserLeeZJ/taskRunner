package taskrunner

import (
	"fmt"
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
	execute  func()
	priority int // 新增字段，数值越大优先级越高
}

// NewMutexManager 创建一个新的 MutexManager
func NewMutexManager() *MutexManager {
	return &MutexManager{
		groups: make(map[string]*GroupQueue),
	}
}

// Enqueue 将任务添加到互斥组队列
func (m *MutexManager) Enqueue(group string, taskName string, execute func(), priority int) {
	m.mu.Lock()
	groupQueue, exists := m.groups[group]
	if !exists {
		groupQueue = &GroupQueue{}
		m.groups[group] = groupQueue
		LogMessage(0, INFO, fmt.Sprintf("为互斥组 %s 创建新的队列", group))
	}
	m.mu.Unlock()

	groupQueue.mu.Lock()
	defer groupQueue.mu.Unlock()

	// 按优先级插入任务，优先级高的在前
	inserted := false
	for i, entry := range groupQueue.queue {
		if priority > entry.priority {
			// 在当前位置插入任务
			groupQueue.queue = append(groupQueue.queue[:i], append([]TaskExecutionEntry{{taskName, execute, priority}}, groupQueue.queue[i:]...)...)
			inserted = true
			break
		}
	}
	if !inserted {
		// 如果没有找到更低优先级的任务，则将任务添加到队列末尾
		groupQueue.queue = append(groupQueue.queue, TaskExecutionEntry{taskName, execute, priority})
	}

	LogMessage(0, INFO, fmt.Sprintf("任务 %s 已加入互斥组 %s 队列，当前队列长度: %d", taskName, group, len(groupQueue.queue)))

	if !groupQueue.running {
		LogMessage(0, INFO, fmt.Sprintf("互斥组 %s 当前没有运行中的任务，开始执行", group))
		go m.executeNext(group)
	} else {
		LogMessage(0, INFO, fmt.Sprintf("互斥组 %s 当前有运行中的任务，新任务已加入队列", group))
	}
}

// executeNext 执行互斥组中的下一个任务
func (m *MutexManager) executeNext(group string) {
	m.mu.Lock()
	groupQueue, exists := m.groups[group]
	m.mu.Unlock()

	if !exists {
		return
	}

	groupQueue.mu.Lock()
	defer groupQueue.mu.Unlock()

	if len(groupQueue.queue) == 0 {
		groupQueue.running = false
		return
	}

	task := groupQueue.queue[0]
	groupQueue.queue = groupQueue.queue[1:]
	groupQueue.running = true

	LogMessage(0, INFO, fmt.Sprintf("开始执行互斥组 %s 中的任务 %s", group, task.taskName))
	go func() {
		task.execute()
		m.Finish(group)
	}()
}

// Finish 设置互斥组的运行状态为完成，并尝试执行下一个任务
func (m *MutexManager) Finish(group string) {
	LogMessage(0, INFO, fmt.Sprintf("互斥组 %s 中的任务执行完成", group))
	go m.executeNext(group)
}

// 我们不再需要 Dequeue 方法，因为任务会在 Enqueue 或 Finish 时自动执行
