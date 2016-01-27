package nsqworker

import "github.com/bitly/go-nsq"

type CqTaskConsumer struct {
	Addr            string
	LookupdAddr     string
	Topic           string
	ConCurrentCount int
	consumer        []*nsq.Consumer
}

var cqConsumer *CqTaskConsumer = &CqTaskConsumer{}

func GetConsumer() *CqTaskConsumer {
	return cqConsumer
}

func (this *CqTaskConsumer) Start() error {
	cfg := nsq.NewConfig()

	for k, v := range WorkerMap {
		consumer, err := nsq.NewConsumer(this.Topic, k, cfg)
		if err != nil {
			panic(err.Error())
			break
		}
		consumer.AddConcurrentHandlers(v, this.ConCurrentCount)
		consumer.ChangeMaxInFlight(this.ConCurrentCount)
		consumer.ConnectToNSQD(this.Addr)

		this.consumer = append(this.consumer, consumer)
	}

	return nil
}

func (this *CqTaskConsumer) Stop() {
	for _, v := range this.consumer {
		v.Stop()
		<-v.StopChan
	}
}
