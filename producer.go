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
	"log"

	"github.com/nsqio/go-nsq"
)

// Producer type
type Producer struct {
	config   *nsq.Config
	Producer *nsq.Producer
}

var (
	producer *Producer
)

func NewProducer(addr string, config *nsq.Config) *Producer {
	var err error
	nsq, err := nsq.NewProducer(addr, config)
	if err != nil {
		panic(err.Error())
	}

	return &Producer{
		config:   config,
		Producer: nsq,
	}
}

// NewNsqProducer get producer
// Deprecated: use NewProducer instead
func NewNsqProducer(addr string, config *nsq.Config) *Producer {
	return NewProducer(addr, config)
}

// GetProducer 返回producer
func GetProducer() *Producer {
	return producer
}

// Stop producer
func (p *Producer) Stop() {
	p.Producer.Stop()
}

// Publish producer
func (p *Producer) Publish(topic string, body []byte) error {
	err := p.Producer.Publish(topic, body)
	if err != nil {
		log.Fatalf("publish message error:[%s],[%s], [%s]", topic, string(body), err.Error())
	}

	return err
}

// MultiPublish 批量发布
func (p *Producer) MultiPublish(topic string, body [][]byte) error {
	err := p.Producer.MultiPublish(topic, body)
	if err != nil {
		log.Fatalf("MultiPublish message error:[%s], [%s]", topic, err.Error())
	}

	return err
}
