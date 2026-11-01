// Package kafkax gom phần cấu hình Kafka dùng chung cho mọi dịch vụ.
package kafkax

import (
	"context"
	"errors"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Brokers đọc danh sách broker từ KAFKA_BROKERS (mặc định localhost:9092)
func Brokers() []string {
	b := os.Getenv("KAFKA_BROKERS")
	if b == "" {
		b = "localhost:9092"
	}
	return strings.Split(b, ",")
}

// NewProducer: acks=all + idempotent (mặc định), nén zstd, linger 5ms
func NewProducer(clientID string, brokers []string, opts ...kgo.Opt) (*kgo.Client, error) {
	base := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ClientID(clientID),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(5 * time.Millisecond),
		kgo.ProducerBatchCompression(kgo.ZstdCompression(), kgo.NoCompression()),
		kgo.RecordDeliveryTimeout(time.Minute),
	}
	return kgo.NewClient(append(base, opts...)...)
}

// NewConsumer: consumer group, commit thủ công, chặn rebalance giữa lô
func NewConsumer(group string, brokers []string, topics []string, opts ...kgo.Opt) (*kgo.Client, error) {
	base := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ClientID(group),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	}
	return kgo.NewClient(append(base, opts...)...)
}

// TopicSpec mô tả một topic cần có
type TopicSpec struct {
	Name       string
	Partitions int32
}

// EnsureTopics tạo topic nếu chưa có (dùng cho môi trường dev/test;
// production nên quản lý topic bằng hạ tầng dưới dạng code)
func EnsureTopics(ctx context.Context, cl *kgo.Client, specs ...TopicSpec) error {
	adm := kadm.NewClient(cl)
	for _, s := range specs {
		_, err := adm.CreateTopic(ctx, s.Partitions, -1, nil, s.Name)
		if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
			return err
		}
	}
	return nil
}

// AllTopics là danh sách topic của hệ thống
var AllTopics = []TopicSpec{
	{"orders", 6}, {"payments", 6}, {"orders.dlq", 1}, {"payments.dlq", 1},
}
