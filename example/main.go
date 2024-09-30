package main

import (
	"fmt"
	"time"

	"github.com/UserLeeZJ/taskrunner"
)

// ExampleTask 示例任务
func ExampleTask() {
	fmt.Println("ExampleTask 执行中...")
	time.Sleep(2 * time.Second)
	fmt.Println("ExampleTask 执行完成")
}

// DependentTask 依赖任务
func DependentTask() {
	fmt.Println("DependentTask 执行中...")
	time.Sleep(1 * time.Second)
	fmt.Println("DependentTask 执行完成")
}

// IndependentTask 独立任务，不属于任何互斥组
func IndependentTask() {
	fmt.Println("IndependentTask 执行中...")
	time.Sleep(3 * time.Second)
	fmt.Println("IndependentTask 执行完成")
}

func main() {
	// 创建 TaskRunner
	runner, err := taskrunner.New(2)
	if err != nil {
		fmt.Printf("创建 TaskRunner 失败: %v\n", err)
		return
	}

	// 设置自定义日志处理器（输出到控制台）
	consoleLogger := func(taskID uint, level taskrunner.LogLevel, message string) {
		fmt.Printf("[%s] TaskID: %d - %s\n", level, taskID, message)
	}
	runner.SetLogHandler(consoleLogger)

	// 注册任务
	err = runner.RegisterTask("ExampleTask", ExampleTask, 10, 5, "group1", []string{})
	if err != nil {
		fmt.Printf("注册 ExampleTask 失败: %v\n", err)
	}

	err = runner.RegisterTask("DependentTask", DependentTask, 15, 5, "group1", []string{"ExampleTask"})
	if err != nil {
		fmt.Printf("注册 DependentTask 失败: %v\n", err)
	}

	err = runner.RegisterTask("IndependentTask", IndependentTask, 20, 5, "", []string{})
	if err != nil {
		fmt.Printf("注册 IndependentTask 失败: %v\n", err)
	}

	// 运行任务调度
	// 调度器已在 New() 中启动

	// 运行一段时间
	time.Sleep(60 * time.Second)

	// 停止 TaskRunner
	runner.Stop()
}
