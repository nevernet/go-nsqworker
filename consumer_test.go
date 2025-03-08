package nsqworker

import (
	"testing"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/stretchr/testify/assert"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()
	assert.NotNil(t, config)
	assert.Equal(t, 30*time.Second, config.ConnectTimeout)
	assert.Equal(t, 5*time.Second, config.RetryInterval)
}

func TestNewConfig(t *testing.T) {
	addr := "127.0.0.1:4150"
	lookupdAddr := "127.0.0.1:4161"
	config := NewConfig(addr, lookupdAddr)

	assert.NotNil(t, config)
	assert.Equal(t, addr, config.Addr)
	assert.Equal(t, lookupdAddr, config.LookupdAddr)
	assert.Equal(t, 30*time.Second, config.ConnectTimeout)
	assert.Equal(t, 5*time.Second, config.RetryInterval)
}

func TestNewConsumer(t *testing.T) {
	tests := []struct {
		name      string
		config    *Config
		nsqConfig *nsq.Config
		wantErr   bool
	}{
		{
			name:      "nil config",
			config:    nil,
			nsqConfig: nsq.NewConfig(),
			wantErr:   true,
		},
		{
			name:      "nil nsq config",
			config:    DefaultConfig(),
			nsqConfig: nil,
			wantErr:   true,
		},
		{
			name:      "valid configs",
			config:    NewConfig("127.0.0.1:4150", "127.0.0.1:4161"),
			nsqConfig: nsq.NewConfig(),
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			consumer, err := NewConsumer(tt.config, tt.nsqConfig)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, consumer)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, consumer)
				consumer.Stop()
			}
		})
	}
}

type mockHandler struct {
	topic string
}

func (h *mockHandler) HandleMessage(message *nsq.Message) error {
	return nil
}

func (h *mockHandler) GetTopic() string {
	return h.topic
}

func TestConsumerWithHandler(t *testing.T) {
	// 清理之前的处理器
	UnregisterHandler("test_handler")

	// 注册一个模拟的处理器
	handler := &mockHandler{topic: "test_topic"}
	err := RegisterConcurrentHandler("test_handler", handler, 2)
	assert.NoError(t, err)

	config := NewConfig("127.0.0.1:4150", "")
	nsqConfig := nsq.NewConfig()

	consumer, err := NewConsumer(config, nsqConfig)
	assert.NoError(t, err)
	assert.NotNil(t, consumer)

	// 测试消费者是否正确创建
	assert.Len(t, consumer.consumers, 1)

	// 测试停止功能
	consumer.Stop()

	// 清理
	UnregisterHandler("test_handler")
}

func TestConsumerWithLookupd(t *testing.T) {
	// 清理之前的处理器
	UnregisterHandler("test_handler")

	handler := &mockHandler{topic: "test_topic"}
	err := RegisterHandler("test_handler", handler)
	assert.NoError(t, err)

	config := NewConfig("", "127.0.0.1:4161")
	config.EnableLookupd = true
	nsqConfig := nsq.NewConfig()

	consumer, err := NewConsumer(config, nsqConfig)
	assert.NoError(t, err)
	assert.NotNil(t, consumer)

	consumer.Stop()

	// 清理
	UnregisterHandler("test_handler")
}
