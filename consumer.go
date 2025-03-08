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
	wg        sync.WaitGroup
}

// NewConfig 创建新的配置
func NewConfig(addr, lookupdAddr string) *Config {
	config := DefaultConfig()
	config.Addr = addr
	config.LookupdAddr = lookupdAddr
	return config
}

// NewConsumer 创建新的消费者
func NewConsumer(config *Config, nsqConfig *nsq.Config) (*Consumer, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	if nsqConfig == nil {
		return nil, fmt.Errorf("nsqConfig cannot be nil")
	}

	ctx, cancel := context.WithCancel(context.Background())
	consumer := &Consumer{
		config:    config,
		nsqConfig: nsqConfig,
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
	if len(handlers) == 0 {
		return fmt.Errorf("no handlers registered")
	}

	concurrents := GetHandlerConCurrent()
	for name, handler := range handlers {
		if err := c.addConsumer(name, handler, concurrents[name]); err != nil {
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
			err = consumer.ConnectToNSQLookupd(c.config.LookupdAddr)
		} else {
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
