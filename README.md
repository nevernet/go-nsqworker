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

	producer := nsqworker.NewNsqProducer(conf.Nsq.Addr, nsq.NewConfig())
	logger.Info("nsq producer initialized.")
	nsqConfig := nsqworker.NewNsqConsumerConfig(conf.Nsq.Addr, conf.Nsq.LookupdAddr, conf.Nsq.Topic, conf.Nsq.ConsumerCount)
	consumer := nsqworker.NewNsqConsumer(nsqConfig, nsq.NewConfig())
	logger.Info("nsq consumer initialized.")

	exitChan := make(chan struct{})
	signalChan := make(chan os.Signal, 1)
	go func() {
		<-signalChan
		close(exitChan)
	}()
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	<-exitChan

	producer.Stop()
	logger.Info("producer stopped.")
	consumer.Stop()
	logger.Info("consumer stopped.")

}
```

create directory  `hanlders` in your project root folder, for example:
`mkdir handlers`

create bootstrap: handlers/bootstrap.go, for example

```go
package handlers

//just for placeholder, in order to init the package workers, this method will be called in main function
func Start() {

}

```

Create custom handler, for example: CqMailHandler
`creaet file handlers/cq_mail_handler.go`

```go
package handlers

import (
	"github.com/nsqio/go-nsq"
	"github.com/nevernet/go-nsqworker"
	"github.com/nevernet/logger"
)

type CqMailHandler struct {
}

func init() {
	//register your worker
	nsqworker.RegisterHandler("cqMail", &CqMailHandler{})
}

//implement HandleMessage
func (this *CqMailHandler) HandleMessage(message *nsq.Message) error {
	logger.Error("handler:[%s], message:[%s]", "cqMail", string(message.Body))
	return nil
}

```
