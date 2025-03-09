// Copyright 2014 Daniel Qin. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nsqworker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nsqio/go-nsq"
)

// ProducerConfig 生产者配置
type ProducerConfig struct {
	// NSQ服务器地址
	Addr string
	// 连接超时时间
	ConnectTimeout time.Duration
	// 重试次数
	RetryCount int
	// 重试间隔
	RetryInterval time.Duration
}

// DefaultProducerConfig 返回默认生产者配置
func DefaultProducerConfig() *ProducerConfig {
	return &ProducerConfig{
		ConnectTimeout: 30 * time.Second,
		RetryCount:     10,
		RetryInterval:  1 * time.Second,
	}
}

// Producer NSQ生产者
type Producer struct {
	config   *ProducerConfig
	nsqConf  *nsq.Config
	producer *nsq.Producer
	mu       sync.RWMutex
}

var (
	defaultProducer *Producer
	once            sync.Once
)

// NewProducer 创建新的生产者实例
func NewProducer(config *ProducerConfig, nsqConfig *nsq.Config) (*Producer, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	if nsqConfig == nil {
		return nil, fmt.Errorf("nsqConfig cannot be nil")
	}

	// 检查Addr是否为空
	if config.Addr == "" {
		return nil, fmt.Errorf("config.Addr cannot be empty")
	}

	// 验证地址格式
	if !IsValidAddress(config.Addr) {
		return nil, fmt.Errorf("invalid NSQ server address format: %s", config.Addr)
	}

	// 复制config，避免外部修改
	configCopy := &ProducerConfig{
		Addr:           config.Addr,
		ConnectTimeout: config.ConnectTimeout,
		RetryCount:     config.RetryCount,
		RetryInterval:  config.RetryInterval,
	}

	p := &Producer{
		config:  configCopy,
		nsqConf: nsqConfig,
	}

	if err := p.connect(); err != nil {
		return nil, err
	}

	return p, nil
}

// connect 连接到NSQ服务器
func (p *Producer) connect() error {
	// 再次检查地址是否为空
	if p.config.Addr == "" {
		return fmt.Errorf("NSQ server address is empty")
	}

	// 验证地址格式
	if !IsValidAddress(p.config.Addr) {
		return fmt.Errorf("invalid NSQ server address format: %s", p.config.Addr)
	}

	nsqProducer, err := nsq.NewProducer(p.config.Addr, p.nsqConf)
	if err != nil {
		return fmt.Errorf("failed to create producer: %v", err)
	}

	p.producer = nsqProducer
	return nil
}

// Stop 停止生产者
func (p *Producer) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.producer != nil {
		p.producer.Stop()
	}
}

// Publish 发布消息到指定主题
func (p *Producer) Publish(ctx context.Context, topic string, body []byte) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.producer == nil {
		return fmt.Errorf("producer is not initialized")
	}

	var lastErr error
	for i := 0; i <= p.config.RetryCount; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := p.producer.Publish(topic, body); err != nil {
				lastErr = err
				if i < p.config.RetryCount {
					time.Sleep(p.config.RetryInterval)
					continue
				}
				return fmt.Errorf("failed to publish message after %d retries: %v", p.config.RetryCount, err)
			}
			return nil
		}
	}
	return lastErr
}

// MultiPublish 批量发布消息到指定主题
func (p *Producer) MultiPublish(ctx context.Context, topic string, bodies [][]byte) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.producer == nil {
		return fmt.Errorf("producer is not initialized")
	}

	var lastErr error
	for i := 0; i <= p.config.RetryCount; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := p.producer.MultiPublish(topic, bodies); err != nil {
				lastErr = err
				if i < p.config.RetryCount {
					time.Sleep(p.config.RetryInterval)
					continue
				}
				return fmt.Errorf("failed to multi-publish messages after %d retries: %v", p.config.RetryCount, err)
			}
			return nil
		}
	}
	return lastErr
}

// GetProducer 返回默认的生产者实例（单例模式）
func GetProducer() *Producer {
	return defaultProducer
}

// InitDefaultProducer 初始化默认生产者
func InitDefaultProducer(config *ProducerConfig, nsqConfig *nsq.Config) error {
	var err error
	once.Do(func() {
		defaultProducer, err = NewProducer(config, nsqConfig)
	})
	return err
}

// Deprecated: use NewProducer instead
func NewNsqProducer(addr string, config *nsq.Config) *Producer {
	// 检查地址是否为空
	if addr == "" {
		panic("NSQ server address cannot be empty")
	}

	// 验证地址格式
	if !IsValidAddress(addr) {
		panic(fmt.Sprintf("invalid NSQ server address format: %s", addr))
	}

	// 使用默认配置
	producerConfig := DefaultProducerConfig()
	producerConfig.Addr = addr

	p, err := NewProducer(producerConfig, config)
	if err != nil {
		panic(err)
	}
	return p
}
