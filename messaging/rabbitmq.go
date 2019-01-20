package messaging

import (
	"github.com/efritz/backoff"
	"github.com/efritz/watchdog"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

type (
	RabbitMqService struct {
		Url         *url.URL
		connection  *amqp.Connection
		watcher     watchdog.Watcher
		channels    []*ChannelConfig
		m           *sync.Mutex
		ready       chan bool
		isConnected *atomic.Value
	}
	QueueOptions struct {
		Exclusive    bool
		Durable      bool
		AutoDelete   bool
		NoWait       bool
		QueueName    string
		Args         amqp.Table
		ConsumerName string
		AutoAck      bool
		NoLocal      bool
	}
	ExchangeOptions struct {
		NoWait        bool
		Durable       bool
		AutoDelete    bool
		Internal      bool
		Mandatory     bool
		Immediate     bool
		RoutingKey    string
		ExchangeName  string
		ExchangeType  string
		QueueName     string
		ContentType   string
		CorrelationId string
		DeliveryMode  uint8
		Args          amqp.Table
	}
	messageReceivedCallback func(delivery *amqp.Delivery)
	ChannelConfig struct {
		Consumer        Consumer         `yaml:"-"`
		QueueOptions    *QueueOptions    `yaml:"queue,omitempty,flow"`
		ExchangeOptions *ExchangeOptions `yaml:"exchange,omitempty,flow"`
		QosOptions      *QosOptions      `yaml:"qos,omitempty,flow"`
		channel         *atomic.Value
		channelMutex    *sync.Mutex
		Initializer     Initializer
	}
	QosOptions struct {
		PrefetchCount int
		PrefetchSize  int
	}
	Consumer func(d *amqp.Delivery)
	Initializer func(config *ChannelConfig, channel *amqp.Channel)
)

func NewQueueOptions() *QueueOptions {
	// Only defining the ones that are not already default of their type (e.g. bool's are false initially)
	return &QueueOptions{
		Args: nil,
	}
}

func NewExchangeOptions() *ExchangeOptions {
	return &ExchangeOptions{
		Args: nil,
		ExchangeType: "fanout",
		DeliveryMode: amqp.Persistent,
	}
}

func NewRabbitMqService(serverUrl string) *RabbitMqService {
	s := &RabbitMqService{
		m:           &sync.Mutex{},
		isConnected: &atomic.Value{},
	}
	s.isConnected.Store(false)

	urlParsed, err := url.ParseRequestURI(serverUrl)
	if err != nil {
		log.Fatal(err)
	}
	s.Url = urlParsed

	s.watcher = watchdog.NewWatcher(s, backoff.NewConstantBackoff(10*time.Second))
	s.ready = make(chan bool)
	return s
}
