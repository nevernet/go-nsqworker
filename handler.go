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
	"fmt"
	"sync"

	"github.com/nsqio/go-nsq"
)

// Handler 是消息处理器接口
type Handler interface {
	HandleMessage(*nsq.Message) error
	GetTopic() string
}

// HandlerRegistry 处理器注册表
type HandlerRegistry struct {
	sync.RWMutex
	handlers   map[string]Handler
	concurrent map[string]int
}

var (
	// 全局处理器注册表实例
	registry = &HandlerRegistry{
		handlers:   make(map[string]Handler),
		concurrent: make(map[string]int),
	}
)

// RegisterHandler 注册消息处理器
func RegisterHandler(handlerName string, handler Handler) error {
	if handlerName == "" || handler == nil {
		return fmt.Errorf("invalid handler name or handler is nil")
	}

	registry.Lock()
	defer registry.Unlock()

	if _, exists := registry.handlers[handlerName]; exists {
		return fmt.Errorf("handler name:[%s] has already been registered", handlerName)
	}

	registry.handlers[handlerName] = handler
	registry.concurrent[handlerName] = 1
	return nil
}

// RegisterConcurrentHandler 注册带并发数的消息处理器
func RegisterConcurrentHandler(handlerName string, handler Handler, concurrent int) error {
	if handlerName == "" || handler == nil {
		return fmt.Errorf("invalid handler name or handler is nil")
	}
	if concurrent < 1 {
		return fmt.Errorf("concurrent must be greater than 0")
	}

	registry.Lock()
	defer registry.Unlock()

	if _, exists := registry.handlers[handlerName]; exists {
		return fmt.Errorf("handler name:[%s] has already been registered", handlerName)
	}

	registry.handlers[handlerName] = handler
	registry.concurrent[handlerName] = concurrent
	return nil
}

// GetHandlers 获取所有已注册的处理器
func GetHandlers() map[string]Handler {
	registry.RLock()
	defer registry.RUnlock()

	// 创建副本以避免外部修改
	handlers := make(map[string]Handler, len(registry.handlers))
	for k, v := range registry.handlers {
		handlers[k] = v
	}
	return handlers
}

// GetHandlerConCurrent 获取所有处理器的并发配置
func GetHandlerConCurrent() map[string]int {
	registry.RLock()
	defer registry.RUnlock()

	// 创建副本以避免外部修改
	concurrent := make(map[string]int, len(registry.concurrent))
	for k, v := range registry.concurrent {
		concurrent[k] = v
	}
	return concurrent
}

// UnregisterHandler 注销一个处理器
func UnregisterHandler(handlerName string) error {
	if handlerName == "" {
		return fmt.Errorf("handler name cannot be empty")
	}

	registry.Lock()
	defer registry.Unlock()

	if _, exists := registry.handlers[handlerName]; !exists {
		return fmt.Errorf("handler name:[%s] not found", handlerName)
	}

	delete(registry.handlers, handlerName)
	delete(registry.concurrent, handlerName)
	return nil
}
