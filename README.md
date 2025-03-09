# Go NSQ Worker

一个 Go 语言的 NSQ 消费者包装器，包含生产者和消费者的封装实现。

## 安装

```bash
go get github.com/nevernet/go-nsqworker/v3
```

## 使用示例

### 1. 创建主程序 (main.go)

```go
package main

import (
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/nsqio/go-nsq"
	"github.com/nevernet/go-nsqworker/v3"
	"your-project/handlers"
)

func main() {
	var err error
	defer func() {
		if err != nil {
			log.Fatalf("Error: %v", err)
		}
	}()

	// 设置使用所有 CPU 核心
	runtime.GOMAXPROCS(runtime.NumCPU())

	// 初始化 handlers
	handlers.Start()

	// 配置信息
	nsqConfig := &struct {
		Addr          string
		LookupdAddr   string
		Topic         string
		ConsumerCount int
	}{
		Addr:          "127.0.0.1:4150",        // NSQ 地址
		LookupdAddr:   "127.0.0.1:4161",        // NSQ Lookupd 地址
		Topic:         "test_topic",            // 主题
		ConsumerCount: 10,                      // 消费者数量
	}

	// 初始化生产者
	var producer *nsqworker.Producer
	producer, err = nsqworker.NewProducer(nsqConfig.Addr, nsq.NewConfig())
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
		return
	}
	log.Printf("NSQ producer initialized")

	// 初始化消费者配置
	consumerConfig := nsqworker.NewConfig(
		nsqConfig.Addr,
		nsqConfig.LookupdAddr,
		nsqConfig.Topic,
		nsqConfig.ConsumerCount,
	)

	// 初始化消费者
	var consumer *nsqworker.Consumer
	consumer, err = nsqworker.NewConsumer(consumerConfig, nsq.NewConfig())
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
		return
	}
	log.Printf("NSQ consumer initialized")

	// 优雅退出
	exitChan := make(chan struct{})
	signalChan := make(chan os.Signal, 1)
	go func() {
		<-signalChan
		close(exitChan)
	}()
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	<-exitChan

	// 停止服务
	producer.Stop()
	log.Printf("Producer stopped")
	consumer.Stop()
	log.Printf("Consumer stopped")
}
```

### 2. 创建 handlers 目录

```bash
mkdir handlers
```

### 3. 创建 handlers/bootstrap.go

```go
package handlers

// Start 初始化所有 handlers
// 这个方法会在 main 函数中被调用
func Start() {
	// 这里可以添加一些初始化代码
}
```

### 4. 创建自定义 handler (handlers/mail_handler.go)

```go
package handlers

import (
	"log"

	"github.com/nsqio/go-nsq"
	"github.com/nevernet/go-nsqworker/v3"
)

// MailHandler 邮件处理器
type MailHandler struct {
}

func init() {
	// 注册处理器，最后一个参数是并发数
	nsqworker.RegisterConcurrentHandler("mail", &MailHandler{}, 10)
}

// HandleMessage 实现消息处理接口
func (h *MailHandler) HandleMessage(message *nsq.Message) error {
	log.Printf("Handler: [mail], Message: [%s]", string(message.Body))
	return nil
}
```

## 配置说明

- `Addr`: NSQ 服务器地址
- `LookupdAddr`: NSQ Lookupd 服务器地址
- `Topic`: 消息主题
- `ConsumerCount`: 消费者数量

## 注意事项

1. 确保 NSQ 服务已经启动
2. 根据实际情况调整配置参数
3. 建议在生产环境中添加适当的错误处理和日志记录
4. Handler 的并发数要根据实际业务需求来设置

## License

MIT
