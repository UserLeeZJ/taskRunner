package taskrunner

import (
	"sync"
)

// Config 配置结构体
type Config struct {
    Workers int // 工作协程数
}

// 配置单例
var (
    configInstance *Config
    once           sync.Once
)

// LoadConfig 加载配置，确保只加载一次
func LoadConfig(cfg Config) {
    once.Do(func() {
        configInstance = &cfg
    })
}

// GetConfig 获取配置实例
func GetConfig() *Config {
    return configInstance
}