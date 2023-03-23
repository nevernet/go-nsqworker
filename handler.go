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

	"github.com/nsqio/go-nsq"
)

// Handler is interface
type Handler interface {
	HandleMessage(*nsq.Message) error
	GetTopic() string
}

var handlerMap = make(map[string]Handler)
var handlerConCurrentMap = make(map[string]int)

// RegisterHandler is func for register the handler
func RegisterHandler(handlerName string, handler Handler) {
	if _, ok := handlerMap[handlerName]; ok {
		panic(fmt.Sprintf("worker name:[%s] has been registered.", handlerName))
	}

	handlerMap[handlerName] = handler
	handlerConCurrentMap[handlerName] = 1
}

// RegisterConcurrentHandler Register handler with concurrent
func RegisterConcurrentHandler(handlerName string, handler Handler, conCurrent int) {
	if _, ok := handlerMap[handlerName]; ok {
		panic(fmt.Sprintf("worker name:[%s] has been registered.", handlerName))
	}

	handlerMap[handlerName] = handler
	handlerConCurrentMap[handlerName] = conCurrent
}

// GetHandlers 获取所有已经注册的handlers
func GetHandlers() map[string]Handler {
	return handlerMap
}

func GetHandlerConCurrent() map[string]int {
	return handlerConCurrentMap
}
