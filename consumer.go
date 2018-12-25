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

// NsqConsumerConfig ...
type NsqConsumerConfig struct {
	Addr            string
	LookupdAddr     string
	Topic           string
	ConCurrentCount int
}

// NsqConsumer type
type NsqConsumer struct {
	consumers []*nsq.Consumer
}

var (
	consumer = &NsqConsumer{}
)

// NewNsqConsumerConfig ...
func NewNsqConsumerConfig(addr, lookupdAddr, topic string, conCurrentCount int) *NsqConsumerConfig {
	config := &NsqConsumerConfig{
		Addr:            addr,
		LookupdAddr:     lookupdAddr,
		Topic:           topic,
		ConCurrentCount: conCurrentCount,
	}

	return config
}

// NewNsqConsumer get instance of consumer
func NewNsqConsumer(config *NsqConsumerConfig, nsqConfig *nsq.Config) *NsqConsumer {
	// 注册每一个worker
	for k, v := range GetHandlers() {
		nsqConsumer, err := nsq.NewConsumer(config.Topic, k, nsqConfig)
		if err != nil {
			panic(err.Error())
		}
		nsqConsumer.AddConcurrentHandlers(v, config.ConCurrentCount)
		nsqConsumer.ChangeMaxInFlight(config.ConCurrentCount)
		nsqConsumer.ConnectToNSQD(config.Addr)

		consumer.consumers = append(consumer.consumers, nsqConsumer)
	}
	return consumer
}

// Stop consumer
func (w *NsqConsumer) Stop() {
	for _, v := range w.consumers {
		v.Stop()
		<-v.StopChan
	}
}
