定义基础的 NSQ 封装

```go
package nsq

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nevernet/go-nsqworker/v3"
	gonsq "github.com/nsqio/go-nsq"
)

// WrapperConfig 配置
type WrapperConfig struct {
	// Consumer 地址
	Addr string
	// Consumer LookupdAddr地址
	LookupdAddr string
	// Producer Addr地址
	ProducerAddr string
	// EnableLookupd 是否启用Lookupd
	EnableLookupd int
}

// Wrapper nsq 封装
type Wrapper struct {
	config       *WrapperConfig
	nsqConfig    *gonsq.Config
	Producer     *nsqworker.Producer // 向后兼容: 导出字段
	Consumer     *nsqworker.Consumer // 向后兼容: 导出字段
	producerOnce sync.Once           // 生产者初始化锁
	consumerOnce sync.Once           // 消费者初始化锁
}

// NSQConfig NSQ 基础配置
type NSQConfig struct {
	LookupdPollInterval time.Duration
	HeartbeatInterval   time.Duration
	MsgTimeout          time.Duration
	MaxInFlight         int
}

// DefaultNSQConfig 返回默认的 NSQ 配置
func DefaultNSQConfig() *NSQConfig {
	return &NSQConfig{
		LookupdPollInterval: 30 * time.Second,
		HeartbeatInterval:   5 * time.Second,
		MsgTimeout:          5 * time.Minute,
		MaxInFlight:         100,
	}
}

// NewWrapper 只创建wrapper实例和基础配置，不进行实际连接
func NewWrapper(config *WrapperConfig) (*Wrapper, error) {
	if config == nil {
		return nil, errors.New("config cannot be nil")
	}

	if config.ProducerAddr == "" {
		return nil, errors.New("producer addr cannot be empty")
	}

	// 验证配置
	if config.EnableLookupd == 1 && config.LookupdAddr == "" {
		return nil, errors.New("lookupd address cannot be empty when lookupd is enabled")
	} else if config.EnableLookupd == 0 && config.Addr == "" {
		return nil, errors.New("addr cannot be empty when lookupd is disabled")
	}

	defaultCfg := DefaultNSQConfig()

	// 创建基础配置
	gonsqConfig := gonsq.NewConfig()
	gonsqConfig.LookupdPollInterval = defaultCfg.LookupdPollInterval
	gonsqConfig.HeartbeatInterval = defaultCfg.HeartbeatInterval
	gonsqConfig.MsgTimeout = defaultCfg.MsgTimeout
	gonsqConfig.MaxInFlight = defaultCfg.MaxInFlight

	return &Wrapper{
		config:    config,
		nsqConfig: gonsqConfig,
	}, nil
}

// InitializeProducer 初始化生产者
func (w *Wrapper) InitializeProducer() error {
	var initErr error
	w.producerOnce.Do(func() {
		if w.Producer != nil {
			return // 已经初始化过
		}

		if w.config.ProducerAddr == "" {
			return // 无需初始化
		}

		defaultConfig := nsqworker.DefaultProducerConfig()
		defaultConfig.Addr = w.config.ProducerAddr

		producer, err := nsqworker.NewProducer(defaultConfig, w.nsqConfig)
		if err != nil {
			initErr = fmt.Errorf("failed to create NSQ producer: %v", err)
			return
		}
		w.Producer = producer
	})
	return initErr
}

// InitializeConsumer 初始化消费者
func (w *Wrapper) InitializeConsumer() error {
	var initErr error
	w.consumerOnce.Do(func() {
		if w.Consumer != nil {
			return // 已经初始化过
		}

		if w.config.Addr == "" {
			return // 无需初始化
		}

		consumerConfig := nsqworker.NewConfig(w.config.Addr, w.config.LookupdAddr)
		consumerConfig.EnableLookupd = w.config.EnableLookupd == 1
		consumer, err := nsqworker.NewConsumer(consumerConfig, w.nsqConfig)
		if err != nil {
			initErr = fmt.Errorf("failed to create NSQ consumer: %v", err)
			return
		}
		w.Consumer = consumer
	})
	return initErr
}
```