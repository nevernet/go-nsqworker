# 定义 NSQ 配置

```go
package config

import (
	// 导入自己的包
    "your-project/wrapper/nsq"
)

var (
	nsqWrapper *nsq.Wrapper
)

func NewNSQ() (*nsq.Wrapper, error) {
	var err error
	nsqWrapper, err = nsq.NewWrapper(GetConfig().Nsq)
	if err != nil {
		logger.Error("failed to create nsq wrapper: %v", err)
		return nil, err
	}

	err = nsqWrapper.InitializeProducer()
	if err != nil {
		logger.Error("failed to initialize nsq producer: %v", err)
		return nil, err
	}

	err = nsqWrapper.InitializeConsumer()
	if err != nil {
		logger.Error("failed to initialize nsq consumer: %v", err)
		return nil, err
	}

	return nsqWrapper, nil
}

func GetNsqWrapper() *nsq.Wrapper {
	return nsqWrapper
}

func CloseNSQ() {
	if nsqWrapper != nil {
		nsqWrapper.Producer.Stop()
		nsqWrapper.Consumer.Stop()
		logger.Info("producer and consumer stopped.")
	}
}

```