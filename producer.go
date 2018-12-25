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
	"github.com/nevernet/logger"
	"github.com/nsqio/go-nsq"
)

// NsqProducer type
type NsqProducer struct {
	config   *nsq.Config
	Producer *nsq.Producer
}

var (
	producer = &NsqProducer{}
)

// NewNsqProducer get producer
func NewNsqProducer(addr string, config *nsq.Config) *NsqProducer {
	var err error
	nsq, err := nsq.NewProducer(addr, config)
	if err != nil {
		panic(err.Error())
	}

	producer.Producer = nsq
	producer.config = config

	return producer
}

// GetProducer 返回producer
func GetProducer() *NsqProducer {
	return producer
}

// Stop producer
func (p *NsqProducer) Stop() {
	p.Producer.Stop()
}

// Publish producer
func (p *NsqProducer) Publish(topic string, body []byte) error {
	err := p.Producer.Publish(topic, body)
	if err != nil {
		logger.Error("publish message error:[%s],[%s], [%s]", topic, string(body), err.Error())
	}

	return err
}

// MultiPublish 批量发布
func (p *NsqProducer) MultiPublish(topic string, body [][]byte) error {
	err := p.Producer.MultiPublish(topic, body)
	if err != nil {
		logger.Error("MultiPublish message error:[%s], [%s]", topic, err.Error())
	}

	return err
}
