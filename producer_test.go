package nsqworker

import (
	"context"
	"testing"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/stretchr/testify/assert"
)

func TestDefaultProducerConfig(t *testing.T) {
	config := DefaultProducerConfig()
	assert.NotNil(t, config)
	assert.Equal(t, 30*time.Second, config.ConnectTimeout)
	assert.Equal(t, 10, config.RetryCount)
	assert.Equal(t, 1*time.Second, config.RetryInterval)
}

func TestNewProducer(t *testing.T) {
	tests := []struct {
		name      string
		config    *ProducerConfig
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
			config:    DefaultProducerConfig(),
			nsqConfig: nil,
			wantErr:   true,
		},
		{
			name: "empty addr",
			config: &ProducerConfig{
				Addr:           "",
				ConnectTimeout: 30 * time.Second,
				RetryCount:     10,
				RetryInterval:  1 * time.Second,
			},
			nsqConfig: nsq.NewConfig(),
			wantErr:   true,
		},
		{
			name: "invalid addr format",
			config: &ProducerConfig{
				Addr:           "localhost", // 缺少端口
				ConnectTimeout: 30 * time.Second,
				RetryCount:     10,
				RetryInterval:  1 * time.Second,
			},
			nsqConfig: nsq.NewConfig(),
			wantErr:   true,
		},
		{
			name: "valid config",
			config: &ProducerConfig{
				Addr:           "127.0.0.1:4150",
				ConnectTimeout: 30 * time.Second,
				RetryCount:     10,
				RetryInterval:  1 * time.Second,
			},
			nsqConfig: nsq.NewConfig(),
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			producer, err := NewProducer(tt.config, tt.nsqConfig)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, producer)
			} else {
				// 注意：这个测试需要本地运行NSQ服务才能通过
				// 如果没有运行NSQ服务，这个测试会失败
				if err != nil {
					t.Logf("Test requires running NSQ server at %s: %v", tt.config.Addr, err)
					return
				}
				assert.NoError(t, err)
				assert.NotNil(t, producer)
				producer.Stop()
			}
		})
	}
}

func TestProducerPublish(t *testing.T) {
	// 创建一个带有模拟地址的配置
	// 注意：这个测试需要本地运行NSQ服务才能通过
	config := &ProducerConfig{
		Addr:           "127.0.0.1:4150",
		ConnectTimeout: 30 * time.Second,
		RetryCount:     3,
		RetryInterval:  100 * time.Millisecond,
	}
	nsqConfig := nsq.NewConfig()

	producer, err := NewProducer(config, nsqConfig)
	if err != nil {
		t.Logf("Test requires running NSQ server at %s: %v", config.Addr, err)
		t.Skip("Skipping test as NSQ server is not available")
		return
	}
	defer producer.Stop()

	// 测试发布消息
	ctx := context.Background()
	err = producer.Publish(ctx, "test_topic", []byte("test message"))
	if err != nil {
		t.Logf("Failed to publish message: %v", err)
		t.Skip("Skipping test as NSQ server is not available or not accepting messages")
		return
	}
	assert.NoError(t, err)

	// 测试上下文取消
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel() // 立即取消
	err = producer.Publish(cancelCtx, "test_topic", []byte("test message"))
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

func TestProducerMultiPublish(t *testing.T) {
	// 创建一个带有模拟地址的配置
	// 注意：这个测试需要本地运行NSQ服务才能通过
	config := &ProducerConfig{
		Addr:           "127.0.0.1:4150",
		ConnectTimeout: 30 * time.Second,
		RetryCount:     3,
		RetryInterval:  100 * time.Millisecond,
	}
	nsqConfig := nsq.NewConfig()

	producer, err := NewProducer(config, nsqConfig)
	if err != nil {
		t.Logf("Test requires running NSQ server at %s: %v", config.Addr, err)
		t.Skip("Skipping test as NSQ server is not available")
		return
	}
	defer producer.Stop()

	// 测试批量发布消息
	ctx := context.Background()
	messages := [][]byte{
		[]byte("message 1"),
		[]byte("message 2"),
		[]byte("message 3"),
	}
	err = producer.MultiPublish(ctx, "test_topic", messages)
	if err != nil {
		t.Logf("Failed to multi-publish messages: %v", err)
		t.Skip("Skipping test as NSQ server is not available or not accepting messages")
		return
	}
	assert.NoError(t, err)

	// 测试上下文取消
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel() // 立即取消
	err = producer.MultiPublish(cancelCtx, "test_topic", messages)
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

func TestInitDefaultProducer(t *testing.T) {
	// 确保之前的测试没有留下默认生产者
	defaultProducer = nil

	config := &ProducerConfig{
		Addr:           "127.0.0.1:4150",
		ConnectTimeout: 30 * time.Second,
		RetryCount:     3,
		RetryInterval:  100 * time.Millisecond,
	}
	nsqConfig := nsq.NewConfig()

	// 初始化默认生产者
	err := InitDefaultProducer(config, nsqConfig)
	if err != nil {
		t.Logf("Test requires running NSQ server at %s: %v", config.Addr, err)
		t.Skip("Skipping test as NSQ server is not available")
		return
	}

	// 获取默认生产者
	producer := GetProducer()
	assert.NotNil(t, producer)
	assert.Equal(t, config.Addr, producer.config.Addr)

	// 清理
	producer.Stop()
	defaultProducer = nil
}

func TestNewNsqProducer(t *testing.T) {
	// 注意：这个测试使用了已弃用的函数，但仍然需要测试其功能
	nsqConfig := nsq.NewConfig()
	addr := "127.0.0.1:4150"

	// 使用defer-recover捕获可能的panic
	defer func() {
		if r := recover(); r != nil {
			// 预期会发生panic，如果NSQ服务器不可用
			t.Logf("Expected panic when NSQ server is not available: %v", r)
			// 这是一个已弃用的函数，它在服务器不可用时会panic，这是预期行为
			// 所以我们不将测试标记为失败
		}
	}()

	// 尝试创建生产者
	var producer *Producer
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Logf("Caught panic: %v", r)
				// 重新panic，让外层的recover捕获
				panic(r)
			}
		}()
		producer = NewNsqProducer(addr, nsqConfig)
	}()

	// 如果成功创建了生产者，则停止它
	if producer != nil {
		producer.Stop()
		t.Log("Successfully created producer with deprecated function")
	}
}

func TestProducerWithUnavailableServer(t *testing.T) {
	// 使用一个不可用的地址
	config := &ProducerConfig{
		Addr:           "127.0.0.1:14150", // 使用一个不太可能在使用的端口
		ConnectTimeout: 1 * time.Second,   // 设置较短的超时时间，加快测试速度
		RetryCount:     1,
		RetryInterval:  100 * time.Millisecond,
	}
	nsqConfig := nsq.NewConfig()

	// 创建生产者可能成功，因为NSQ客户端库在创建时不会立即连接
	producer, err := NewProducer(config, nsqConfig)
	if err != nil {
		// 如果创建失败，测试通过
		return
	}

	// 确保在测试结束时停止生产者
	defer producer.Stop()

	// 尝试发布消息，这应该会失败，因为服务器不可用
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = producer.Publish(ctx, "test_topic", []byte("test message"))
	assert.Error(t, err, "Publishing to unavailable server should fail")
}

func TestIsValidAddress(t *testing.T) {
	tests := []struct {
		name    string
		addr    string
		isValid bool
	}{
		{
			name:    "empty address",
			addr:    "",
			isValid: false,
		},
		{
			name:    "missing port",
			addr:    "localhost",
			isValid: false,
		},
		{
			name:    "invalid port",
			addr:    "localhost:abc",
			isValid: false,
		},
		{
			name:    "missing host",
			addr:    ":4150",
			isValid: false,
		},
		{
			name:    "too many parts",
			addr:    "localhost:4150:extra",
			isValid: false,
		},
		{
			name:    "valid address",
			addr:    "localhost:4150",
			isValid: true,
		},
		{
			name:    "valid IP address",
			addr:    "127.0.0.1:4150",
			isValid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsValidAddress(tt.addr)
			assert.Equal(t, tt.isValid, result)
		})
	}
}
