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

	// 这里要导入你自己的 handlers 包
	_ "your-project/handlers"
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

	// 配置信息 应该放到自己的config里面
	// nsqConfig := &struct {
	// 	Addr          string
	// 	LookupdAddr   string
	// 	Topic         string
	// 	ConsumerCount int
	// }{
	// 	Addr:          "127.0.0.1:4150",        // NSQ 地址
	// 	LookupdAddr:   "127.0.0.1:4161",        // NSQ Lookupd 地址
	// 	Topic:         "test_topic",            // 主题
	// 	ConsumerCount: 10,                      // 消费者数量
	// }

	// 初始化 NSQ 配置, 导入自己的config包
	nsqWrapper, err := config.NewNSQ()
	if err != nil {
		log.Error("failed to create nsq wrapper: %v", err)
		return
	}
	log.Printf("NSQ producer initialized")

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
	config.CloseNSQ()
}
```

## nsq-wrapper.go
[查看代码](docs/nsq-wrapper.md)

## nsq-config.go
[查看代码](docs/nsq-config.md)

## 创建workere(handlers/approve_post_worker.go)
[查看代码](docs/worker.md)

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
