package rabbitmq

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	EventsExchange   = "orders.events"   // topic: mọi sự kiện nghiệp vụ
	UnroutedExchange = "orders.unrouted" // alternate exchange (AE)
	UnroutedQueue    = "orders.unrouted"
)

// DeclareEvents khai báo exchange dùng chung (idempotent).
func DeclareEvents(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(UnroutedExchange, amqp.ExchangeFanout,
		true, false, false, false, nil); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(UnroutedQueue, true, false, false, false,
		amqp.Table{amqp.QueueTypeArg: amqp.QueueTypeQuorum}); err != nil {
		return err
	}
	err := ch.QueueBind(UnroutedQueue, "", UnroutedExchange, false, nil)
	if err != nil {
		return err
	}
	return ch.ExchangeDeclare(EventsExchange, amqp.ExchangeTopic,
		true, false, false, false, amqp.Table{"alternate-exchange": UnroutedExchange})
}

// ServiceQueues mô tả bộ queue của một service tiêu thụ sự kiện:
//
//	<svc>.q                hàng đợi chính (quorum), bind vào orders.events
//	<svc>.retry.<delay>    hàng đợi chờ (TTL) -> hết hạn thì dead-letter về <svc>.q
//	<svc>.parking-lot      nơi chứa message hỏng/hết lượt thử (DLX của <svc>.q)
type ServiceQueues struct {
	Service  string
	Bindings []string
	Delays   []time.Duration
}

func (s ServiceQueues) Main() string    { return s.Service + ".q" }
func (s ServiceQueues) Parking() string { return s.Service + ".parking-lot" }
func (s ServiceQueues) Retry(i int) string {
	return fmt.Sprintf("%s.retry.%s", s.Service, s.Delays[i])
}

// Declare khai báo toàn bộ topology của service (idempotent, chạy mỗi lần kết nối).
func (s ServiceQueues) Declare(ch *amqp.Channel) error {
	if err := DeclareEvents(ch); err != nil {
		return err
	}
	quorum := func(name string, args amqp.Table) error {
		args[amqp.QueueTypeArg] = amqp.QueueTypeQuorum
		_, err := ch.QueueDeclare(name, true, false, false, false, args)
		return err
	}
	if err := quorum(s.Parking(), amqp.Table{}); err != nil {
		return err
	}
	// Reject(requeue=false) hoặc vượt delivery-limit -> dead-letter sang parking-lot.
	if err := quorum(s.Main(), amqp.Table{
		"x-delivery-limit":          int64(5),
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": s.Parking(),
	}); err != nil {
		return err
	}
	for _, key := range s.Bindings {
		if err := ch.QueueBind(s.Main(), key, EventsExchange, false, nil); err != nil {
			return err
		}
	}
	for i, d := range s.Delays {
		if err := quorum(s.Retry(i), amqp.Table{
			amqp.QueueMessageTTLArg:     d.Milliseconds(),
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": s.Main(),
		}); err != nil {
			return err
		}
	}
	return nil
}
