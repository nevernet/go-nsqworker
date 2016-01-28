# A golang nsq consumer wrapper. 

include one producer and consumer wrapper. 

# demo

```go
func main() {
	var err error
	defer func() {
		if err != nil {
			logger.Error(err.Error())
		}

		logger.Close()
	}()

	runtime.GOMAXPROCS(runtime.NumCPU())

	workers.Start()

	producer := nsqworker.GetProducer()
	producer.Addr = conf.Nsq.Addr
	err = producer.Start()
	if err != nil {
		panic(err.Error())
		return
	}
	logger.Info("cq producer initialized.")
	consumer := nsqworker.GetConsumer()
	consumer.Addr = conf.Nsq.Addr
	consumer.LookupdAddr = conf.Nsq.LookupdAddr
	consumer.Topic = conf.Nsq.Topic
	consumer.ConCurrentCount = conf.Nsq.ConsumerCount
	err = consumer.Start()
	if err != nil {
		panic(err.Error())
		return
	}
	logger.Info("cq consumer initialized.")

	exitChan := make(chan struct{})
	signalChan := make(chan os.Signal, 1)
	go func() {
		<-signalChan
		close(exitChan)
	}()
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	<-exitChan

	nsqworker.GetProducer().Stop()
	logger.Info("cq producer stopped.")
	nsqworker.GetConsumer().Stop()
	logger.Info("cq consumer stopped.")

}
```
