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

	"github.com/bitly/go-nsq"
)

// NsqWorker is interface
type NsqWorker interface {
	HandleMessage(*nsq.Message) error
}

var workerMap = make(map[string]NsqWorker)

// RegisterWorker is func for register the worker
func RegisterWorker(workerName string, worker NsqWorker) {
	if _, ok := workerMap[workerName]; ok {
		panic(fmt.Sprintf("worker name:[%s] has been duplicated.", workerName))
	}
	workerMap[workerName] = worker
}
