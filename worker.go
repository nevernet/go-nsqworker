package nsqworker

import (
	"fmt"

	"github.com/bitly/go-nsq"
)

type NsqWorker interface {
	HandleMessage(*nsq.Message) error
}

var WorkerMap map[string]NsqWorker = make(map[string]NsqWorker)

func RegisterWorker(workerName string, worker NsqWorker) {
	if _, ok := WorkerMap[workerName]; ok {
		panic(fmt.Sprintf("worker name:[%s] has been duplicated.", workerName))
	}
	WorkerMap[workerName] = worker
}
