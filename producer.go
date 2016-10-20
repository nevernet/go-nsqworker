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
	"github.com/bitly/go-nsq"
	"github.com/nevernet/logger"
)

// NsqProducer type
type NsqProducer struct {
	Addr     string
	Producer *nsq.Producer
}

var producer = &NsqProducer{}

// GetProducer get producer
func GetProducer() *NsqProducer {
	return producer
}

// Start producer
func (pds *NsqProducer) Start() error {
	var err error
	cfg := nsq.NewConfig()
	pds.Producer, err = nsq.NewProducer(pds.Addr, cfg)
	if err != nil {
		return err
	}

	return nil
}

// Stop producer
func (pds *NsqProducer) Stop() {
	pds.Producer.Stop()
}

// Publish producer
func (pds *NsqProducer) Publish(topic string, body []byte) error {
	err := pds.Producer.Publish(topic, body)
	if err != nil {
		logger.Error("publish message error:[%s],[%s], [%s]", topic, string(body), err.Error())
	}

	return err
}
