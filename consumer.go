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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nsqio/go-nsq"
)

// Config NSQ消费者配置
type Config struct {
	// NSQ服务器地址
	Addr string
	// NSQ Lookupd服务器地址
	LookupdAddr string
	// 是否启用Lookupd服务发现
	EnableLookupd bool
	// 连接超时时间
	ConnectTimeout time.Duration
	// 重试间隔
	RetryInterval time.Duration
}

// DefaultConfig 返回默认配置
func DefaultConfig() *Config {
	return &Config{
		ConnectTimeout: 30 * time.Second,
		RetryInterval:  5 * time.Second,
	}
}

// Consumer NSQ消费者
type Consumer struct {
	config    *Config
	nsqConfig *nsq.Config
	consumers []*nsq.Consumer
	ctx       context.Context
	cancel    context.CancelFunc
}

// NewConfig 创建新的配置
func NewConfig(addr, lookupdAddr string) *Config {
	config := DefaultConfig()

	// 验证地址格式
	if addr != "" && !isValidAddress(addr) {
		// 如果地址格式不正确，记录警告但不阻止创建
		fmt.Printf("Warning: invalid nsqd address format: %s, should be host:port\n", addr)
	}

	if lookupdAddr != "" && !isValidAddress(lookupdAddr) {
		// 如果地址格式不正确，记录警告但不阻止创建
		fmt.Printf("Warning: invalid lookupd address format: %s, should be host:port\n", lookupdAddr)
	}

	config.Addr = addr
	config.LookupdAddr = lookupdAddr
	return config
}

// NewConsumer 创建新的消费者
func NewConsumer(config *Config, nsqConfig *nsq.Config) (*Consumer, error) {
	if config == nil {
		return nil, errors.New("config cannot be nil")
	}
	if nsqConfig == nil {
		return nil, errors.New("nsqConfig cannot be nil")
	}

	// 验证地址配置
	if !config.EnableLookupd {
		// 如果不使用Lookupd，则必须提供有效的NSQ地址
		if config.Addr == "" {
			return nil, errors.New("nsqd address is required when not using lookupd")
		}
		if !isValidAddress(config.Addr) {
			return nil, fmt.Errorf("invalid nsqd address format: %s, should be host:port", config.Addr)
		}
	} else {
		// 如果使用Lookupd，则必须提供有效的Lookupd地址
		if config.LookupdAddr == "" {
			return nil, errors.New("lookupd address is required when using lookupd")
		}
		if !isValidAddress(config.LookupdAddr) {
			return nil, fmt.Errorf("invalid lookupd address format: %s, should be host:port", config.LookupdAddr)
		}
	}

	// 创建Config的副本，避免外部修改影响内部状态
	configCopy := &Config{
		Addr:           config.Addr,
		LookupdAddr:    config.LookupdAddr,
		EnableLookupd:  config.EnableLookupd,
		ConnectTimeout: config.ConnectTimeout,
		RetryInterval:  config.RetryInterval,
	}

	// 创建nsqConfig的副本
	nsqConfigCopy := nsq.NewConfig()
	*nsqConfigCopy = *nsqConfig

	ctx, cancel := context.WithCancel(context.Background())
	consumer := &Consumer{
		config:    configCopy,
		nsqConfig: nsqConfigCopy,
		ctx:       ctx,
		cancel:    cancel,
	}

	if err := consumer.initConsumers(); err != nil {
		cancel()
		return nil, err
	}

	return consumer, nil
}

// initConsumers 初始化所有消费者
func (c *Consumer) initConsumers() error {
	handlers := GetHandlers()
	// 如果没有注册任何处理器，直接返回成功
	if len(handlers) == 0 {
		return nil
	}

	concurrents := GetHandlerConCurrent()
	for name, handler := range handlers {
		// 获取并发数，如果未配置则默认为1
		concurrent := 1
		if val, exists := concurrents[name]; exists {
			concurrent = val
		}
		if err := c.addConsumer(name, handler, concurrent); err != nil {
			return fmt.Errorf("failed to add consumer %s: %v", name, err)
		}
	}
	return nil
}

// addConsumer 添加单个消费者
func (c *Consumer) addConsumer(name string, handler Handler, concurrent int) error {
	consumer, err := nsq.NewConsumer(handler.GetTopic(), name, c.nsqConfig)
	if err != nil {
		return fmt.Errorf("create consumer failed: %v", err)
	}

	consumer.AddConcurrentHandlers(handler, concurrent)

	// 使用context控制连接超时
	connectCtx, cancel := context.WithTimeout(c.ctx, c.config.ConnectTimeout)
	defer cancel()

	errChan := make(chan error, 1)
	go func() {
		var err error
		if c.config.EnableLookupd {
			if c.config.LookupdAddr == "" {
				err = fmt.Errorf("lookupd address is empty")
				errChan <- err
				return
			}
			// 验证地址格式
			if !isValidAddress(c.config.LookupdAddr) {
				err = fmt.Errorf("invalid lookupd address format: %s, should be host:port", c.config.LookupdAddr)
				errChan <- err
				return
			}
			err = consumer.ConnectToNSQLookupd(c.config.LookupdAddr)
		} else {
			if c.config.Addr == "" {
				err = fmt.Errorf("nsqd address is empty")
				errChan <- err
				return
			}
			// 验证地址格式
			if !isValidAddress(c.config.Addr) {
				err = fmt.Errorf("invalid nsqd address format: %s, should be host:port", c.config.Addr)
				errChan <- err
				return
			}
			err = consumer.ConnectToNSQD(c.config.Addr)
		}
		errChan <- err
	}()

	select {
	case err := <-errChan:
		if err != nil {
			consumer.Stop()
			return fmt.Errorf("connect failed: %v", err)
		}
	case <-connectCtx.Done():
		consumer.Stop()
		return fmt.Errorf("connect timeout")
	}

	c.consumers = append(c.consumers, consumer)
	return nil
}

// isValidAddress 验证地址格式是否正确
func isValidAddress(addr string) bool {
	// 简单验证地址格式，确保包含主机和端口
	// 格式应该是 host:port
	if addr == "" {
		return false
	}

	// 检查地址中是否包含冒号，并且冒号后面有端口号
	parts := strings.Split(addr, ":")
	if len(parts) != 2 {
		return false
	}

	// 检查主机部分不为空
	if parts[0] == "" {
		return false
	}

	// 检查端口部分是数字
	_, err := strconv.Atoi(parts[1])
	return err == nil
}

// Stop 优雅停止所有消费者
func (c *Consumer) Stop() {
	c.cancel()

	// 创建一个WaitGroup来等待所有消费者停止
	var wg sync.WaitGroup
	for _, consumer := range c.consumers {
		wg.Add(1)
		go func(cons *nsq.Consumer) {
			defer wg.Done()
			cons.Stop()
			<-cons.StopChan
		}(consumer)
	}
	// 这里使用协程来停止每个消费者是为了实现并行退出，提高效率。
	// 1. 并行停止：同时停止多个消费者，而不是串行等待每个消费者停止
	// 2. 非阻塞：每个消费者的Stop()调用和等待StopChan都可能需要一定时间
	// 3. 优雅关闭：通过等待StopChan确保每个消费者完全停止所有处理
	// 4. 资源释放：使用WaitGroup确保所有消费者都已正确停止后才继续执行

	// 等待所有消费者停止，或者超时
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// 所有消费者都已经正常停止
	case <-time.After(30 * time.Second):
		// 超时，强制退出
	}
}

// Deprecated: use NewConfig instead
func NewNsqConsumerConfig(addr, lookupdAddr, topic string, conCurrentCount int) *Config {
	return NewConfig(addr, lookupdAddr)
}

// Deprecated: use NewConsumer instead
func NewNsqConsumer(config *Config, nsqConfig *nsq.Config) *Consumer {
	consumer, err := NewConsumer(config, nsqConfig)
	if err != nil {
		panic(err)
	}
	return consumer
}
