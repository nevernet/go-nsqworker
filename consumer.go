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
	"github.com/nsqio/go-nsq"
)

// Config ...
type Config struct {
	Addr        string
	LookupdAddr string

	// 是否使用lookupd
	EnableLookupd int
}

// Consumer type
type Consumer struct {
	consumers []*nsq.Consumer
}

var ()

func NewConfig(addr, lookupdAddr, topic string, conCurrentCount int) *Config {
	config := &Config{
		Addr:        addr,
		LookupdAddr: lookupdAddr,
	}

	return config
}

func NewConsumer(config *Config, nsqConfig *nsq.Config) *Consumer {
	consumer := &Consumer{}

	handlerConCurrentMap := GetHandlerConCurrent()
	// 注册每一个worker
	for k, v := range GetHandlers() {
		nsqConsumer, err := nsq.NewConsumer(v.GetTopic(), k, nsqConfig)
		if err != nil {
			panic(err.Error())
		}
		// nsqConsumer.AddHandler(v)
		conCurrent := 1
		if xConCurrent, ok := handlerConCurrentMap[k]; ok {
			conCurrent = xConCurrent
		}

		// pay attention: the handler should not be stuck, and should return value(nil) asap
		// otherwise the concurrent policy will not work properly
		nsqConsumer.AddConcurrentHandlers(v, conCurrent)
		if config.EnableLookupd == 1 {
			nsqConsumer.ConnectToNSQLookupd(config.LookupdAddr)
		} else {
			nsqConsumer.ConnectToNSQD(config.Addr)
		}

		consumer.consumers = append(consumer.consumers, nsqConsumer)
	}

	return consumer
}

// Stop consumer
func (w *Consumer) Stop() {
	for _, v := range w.consumers {
		v.Stop()
		<-v.StopChan
	}
}

// NewNsqConsumerConfig ...
// Deprecated: use NewConfig instead
func NewNsqConsumerConfig(addr, lookupdAddr, topic string, conCurrentCount int) *Config {
	return NewConfig(addr, lookupdAddr, topic, conCurrentCount)
}

// NewNsqConsumer get instance of consumer
// Deprecated: use NewConsumer instead
func NewNsqConsumer(config *Config, nsqConfig *nsq.Config) *Consumer {
	return NewConsumer(config, nsqConfig)
}
