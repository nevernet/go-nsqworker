package nsqworker

import (
	"github.com/bitly/go-nsq"
	"github.com/nevernet/logger"
)

type NsqProducer struct {
	Addr     string
	Producer *nsq.Producer
}

var producer *NsqProducer = &NsqProducer{}

func GetProducer() *NsqProducer {
	return producer
}

func (this *NsqProducer) Start() error {
	var err error
	cfg := nsq.NewConfig()
	this.Producer, err = nsq.NewProducer(this.Addr, cfg)
	if err != nil {
		return err
	}

	return nil
}

func (this *NsqProducer) Stop() {
	this.Producer.Stop()
}

func (this *NsqProducer) Publish(topic string, body []byte) error {
	err := this.Producer.Publish(topic, body)
	if err != nil {
		logger.Error("publish message error:[%s],[%s], [%s]", topic, string(body), err.Error())
	}

	return err
}
