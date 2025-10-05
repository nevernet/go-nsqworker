# 创建自己的worker(handler)

```go

package post

import (
	"encoding/json"

	// 导入自己的包

	"github.com/nevernet/go-nsqworker/v3"
	"github.com/nsqio/go-nsq"
)

// ApprovePostWorker ...
type ApprovePostWorker struct{}

func init() {
	nsqworker.RegisterHandler("approve_post_worker", &ApprovePostWorker{})
}

// 定义使用的 Topic
func (w *ApprovePostWorker) GetTopic() string {
    // 返回自己的常量定义即可
	return nsqsvr.NsqTopicIotServer
}

// HandleMessage ...
func (w *ApprovePostWorker) HandleMessage(message *nsq.Message) error {
	logger.Info("handler:[%s], message:[%s]", "ApprovePostWorker", string(message.Body))

	var actionData nsqsvr.NSQAction

	err := json.Unmarshal(message.Body, &actionData)
	if err != nil {
		logger.Error("ApprovePostWorker handler failed:%s", err.Error())
		return nil
	}

	// 只处理subject
	if actionData.ActionName != nsqsvr.NSQActionApprovePost {
		return nil
	}

	if actionData.Data == nil {
		logger.Error("ApprovePostWorker process error: data is nil")
		return nil
	}

	// 做类型判定
	x, ok := actionData.Data.(string)
	if !ok {
		logger.Error("process ApprovePostWorker `data` is not string: %#v", actionData.Data)
		return nil
	}

    // 调用自己的消费者处理实际业务逻辑
	err = post.NewApprovePostConsumer().Process(x)
	if err != nil {
		logger.Error("ApprovePostWorker process  error: %v", err.Error())
		return nil
	}

	message.Finish()
	return nil
}

```