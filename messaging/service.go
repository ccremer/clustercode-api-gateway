package messaging

import (
	"github.com/efritz/backoff"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"sync"
	"sync/atomic"
	"time"
)

// This method is part of an interface, do not call it manually!
func (s *RabbitMqService) Retry() bool {
	conn, err := s.connect()
	if err != nil {
		return false
	}

	errRec := make(chan *amqp.Error)
	go func() {
		for msg := range errRec {
			log.Error(msg)
			s.connection = nil
			s.watcher.Check()
			s.isConnected.Store(false)
		}
	}()
	conn.NotifyClose(errRec)

	s.connection = conn
	s.isConnected.Store(true)
	for _, config := range s.channels {
		s.createChannelAndInitialize(config)
	}
	return true
}

func (s *RabbitMqService) getConnection() *amqp.Connection {
	if !s.IsConnected() {
		<-s.ready
	}
	return s.connection
}

func (s *RabbitMqService) Start(configs ...*ChannelConfig) {
	<-s.watcher.Start()
	for _, config := range configs {
		s.AddChannelConfig(config)
	}
}

func (s *RabbitMqService) connect() (*amqp.Connection, error) {
	// we don't want to log the credentials
	urlStripped := s.Url.Scheme + "://" + s.Url.Host + s.Url.Path

	logEntry := log.WithField("url", urlStripped)
	logEntry.Debug("connecting to RabbitMQ server")
	conn, err := amqp.Dial(s.Url.String())
	if err == nil {
		logEntry.Info("connected to RabbitMQ server")
	} else {
		log.WithFields(log.Fields{
			"url":   urlStripped,
			"error": err,
			"help":  "credentials have been removed from URL in the log",
		}).Error("could not connect to RabbitMQ server")
		return nil, err
	}

	return conn, nil
}

func (s *RabbitMqService) IsConnected() bool {
	return s.isConnected.Load().(bool)
}

var b = backoff.NewConstantBackoff(10 * time.Second)

func (s *RabbitMqService) Publish(config *ChannelConfig, payload string) {
	config.channelMutex.Lock()
	defer config.channelMutex.Unlock()

	logEntry := log.WithFields(log.Fields{
		"queue_name":    config.ExchangeOptions.QueueName,
		"exchange_name": config.ExchangeOptions.ExchangeName,
		"routing_key":   config.ExchangeOptions.RoutingKey,
	})
	logEntry.Debug("sending message")

	retry := true
	for retry {
		if s.IsConnected() {
			err := publishOnChannel(config.ExchangeOptions, config.channel.Load().(*amqp.Channel), payload)
			if err == nil {
				retry = false
			} else {
				logEntry.
					WithField("help", "This was probably a network failure. Will retry until succeeded").
					Error(err)
				time.Sleep(b.NextInterval())
			}
		} else {
			time.Sleep(b.NextInterval())
		}
	}
	logEntry.Debug("sent message successfully")
}

func (s *RabbitMqService) createChannelAndInitialize(config *ChannelConfig) {
	s.m.Lock()
	defer s.m.Unlock()
	ch := s.createChannelOrFail()
	config.channel.Store(ch)
	config.Initializer(config, ch)
}

func (s *RabbitMqService) AddChannelConfig(config *ChannelConfig) {
	s.channels = append(s.channels, config)
	config.channelMutex = &sync.Mutex{}
	config.channel = &atomic.Value{}

	if config.Initializer == nil {
		config.Initializer = defaultChannelInitializer
	}

	s.createChannelAndInitialize(config)
}
