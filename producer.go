package nsqworker

import (
	"github.com/bitly/go-nsq"
	"github.com/nevernet/logger"
)

type CqTaskProducer struct {
	Addr     string
	Producer *nsq.Producer
}

var cqProdcuer *CqTaskProducer = &CqTaskProducer{}

func GetProducer() *CqTaskProducer {
	return cqProdcuer
}

func (this *CqTaskProducer) Start() error {
	var err error
	cfg := nsq.NewConfig()
	this.Producer, err = nsq.NewProducer(this.Addr, cfg)
	if err != nil {
		return err
	}

	return nil
}

func (this *CqTaskProducer) Stop() {
	this.Producer.Stop()
}

func (this *CqTaskProducer) Publish(topic string, body []byte) error {
	err := this.Producer.Publish(topic, body)
	if err != nil {
		logger.Error("publish message error:[%s],[%s], [%s]", topic, string(body), err.Error())
	}

	return err
}
