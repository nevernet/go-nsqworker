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

import "github.com/bitly/go-nsq"

// NsqConsumer type
type NsqConsumer struct {
	Addr            string
	LookupdAddr     string
	Topic           string
	ConCurrentCount int
	consumer        []*nsq.Consumer
}

var consumer = &NsqConsumer{}

// GetConsumer get instance of consumer
func GetConsumer() *NsqConsumer {
	return consumer
}

// Start the consumer
func (cns *NsqConsumer) Start() error {
	cfg := nsq.NewConfig()

	for k, v := range workerMap {
		consumer, err := nsq.NewConsumer(cns.Topic, k, cfg)
		if err != nil {
			panic(err.Error())
		}
		consumer.AddConcurrentHandlers(v, cns.ConCurrentCount)
		consumer.ChangeMaxInFlight(cns.ConCurrentCount)
		consumer.ConnectToNSQD(cns.Addr)

		cns.consumer = append(cns.consumer, consumer)
	}

	return nil
}

// Stop consumer
func (cns *NsqConsumer) Stop() {
	for _, v := range cns.consumer {
		v.Stop()
		<-v.StopChan
	}
}
