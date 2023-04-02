# A golang nsq consumer wrapper.

include one producer and consumer wrapper.

# demo

```go
func main() {
	var err error
	defer func() {
		if err != nil {
			log.Fatalf(err.Error())
		}

	}()

	runtime.GOMAXPROCS(runtime.NumCPU())

	producer := nsqworker.NewProducer(conf.Nsq.Addr, nsq.NewConfig())
	log.Printf("nsq producer initialized.")
	nsqConfig := nsqworker.NewConfig(conf.Nsq.Addr, conf.Nsq.LookupdAddr, conf.Nsq.Topic, conf.Nsq.ConsumerCount)
	consumer := nsqworker.NewConsumer(nsqConfig, nsq.NewConfig())
	log.Printf("nsq consumer initialized.")

	exitChan := make(chan struct{})
	signalChan := make(chan os.Signal, 1)
	go func() {
		<-signalChan
		close(exitChan)
	}()
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	<-exitChan

	producer.Stop()
	log.Printf("producer stopped.")
	consumer.Stop()
	log.Printf("consumer stopped.")

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
)

type CqMailHandler struct {
}

func init() {
	//register your worker
	nsqworker.RegisterConcurrentHandler("cqMail", &CqMailHandler{}, 10)
}

//implement HandleMessage
func (this *CqMailHandler) HandleMessage(message *nsq.Message) error {
	log.Fatalf("handler:[%s], message:[%s]", "cqMail", string(message.Body))
	return nil
}

```
