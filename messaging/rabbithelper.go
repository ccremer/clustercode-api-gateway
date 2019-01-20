package messaging

import (
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

func (s *RabbitMqService) createChannelOrFail() *amqp.Channel {
	if channel, err := s.tryCreateChannel(); err == nil {
		return channel
	} else {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("failed to open channel")
		return channel
	}
}

func (s *RabbitMqService) tryCreateChannel() (*amqp.Channel, error) {
	log.Debugf("opening a new channel")
	return s.getConnection().Channel()
}

func createQueueOrFail(o *QueueOptions, channel *amqp.Channel) amqp.Queue {
	if q, err := tryCreateQueue(o, channel); err == nil {
		return q
	} else {
		log.WithFields(log.Fields{
			"queue_name": o.QueueName,
			"error":      err,
		}).Fatal("failed to create queue")
		return q
	}
}

func tryCreateQueue(o *QueueOptions, channel *amqp.Channel) (amqp.Queue, error) {
	log.WithField("queue_name", o.QueueName).Debug("creating queue")
	return channel.QueueDeclare(
		o.QueueName,
		o.Durable,
		o.AutoDelete,
		o.Exclusive,
		o.NoWait,
		o.Args,
	)
}

func createExchangeOrFail(o *ExchangeOptions, channel *amqp.Channel) {
	if err := tryCreateExchange(o, channel); err != nil {
		log.WithFields(log.Fields{
			"exchange_name": o.ExchangeName,
			"error":         err,
		}).Fatal("failed to create exchange")
	}
}

func tryCreateExchange(o *ExchangeOptions, channel *amqp.Channel) error {
	log.WithField("exchange_name", o.ExchangeName).Debug("creating exchange")
	return channel.ExchangeDeclare(
		o.ExchangeName,
		o.ExchangeType,
		o.Durable,
		o.AutoDelete,
		o.Internal,
		o.NoWait,
		o.Args)
}

func bindToExchange(o *ExchangeOptions, channel *amqp.Channel) {
	log.WithFields(log.Fields{
		"queue_name":    o.QueueName,
		"exchange_name": o.ExchangeName,
	}).Debug("binding queue to exchange")

	err := channel.QueueBind(
		o.QueueName,
		o.RoutingKey,
		o.ExchangeName,
		o.NoWait,
		o.Args)

	if err != nil {
		log.WithFields(log.Fields{
			"queue_name":    o.QueueName,
			"exchange_name": o.ExchangeName,
			"error":         err,
		}).Fatal("failed to bind queue")
	}
}

func createConsumerOrFail(o *QueueOptions, channel *amqp.Channel) <-chan amqp.Delivery {
	if msgs, err := tryCreateConsumer(o, channel); err == nil {
		return msgs
	} else {
		log.WithFields(log.Fields{
			"queue_name": o.QueueName,
			"error":      err,
		}).Fatal("failed to consume queue")
		return msgs
	}
}

func tryCreateConsumer(o *QueueOptions, channel *amqp.Channel) (<-chan amqp.Delivery, error) {
	return channel.Consume(
		o.QueueName,
		o.ConsumerName,
		o.AutoAck,
		o.Exclusive,
		o.NoLocal,
		o.NoWait,
		o.Args,
	)
}

func setQos(o *QosOptions, channel *amqp.Channel) {
	global := false
	err := channel.Qos(
		o.PrefetchCount,
		o.PrefetchSize,
		global,
	)
	if err != nil {
		log.WithField("error", err).Fatal("failed to set QoS")
	}
}

func beginConsuming(msgs <-chan amqp.Delivery, callback messageReceivedCallback) {
	go func(msgs <-chan amqp.Delivery) {
		for msg := range msgs {
			log.WithFields(log.Fields{
				"routing_key": msg.RoutingKey,
				"correlation_id": msg.CorrelationId,
				"reply_to": msg.ReplyTo,
				"consumer_tag": msg.ConsumerTag,
			}).Debug("received message")
			callback(&msg)
		}
	}(msgs)
}

func publishOnChannel(options *ExchangeOptions, channel *amqp.Channel, payload string) error {
	return channel.Publish(
		options.ExchangeName,
		options.RoutingKey,
		options.Mandatory,
		options.Immediate,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/xml",
			Body:         []byte(payload),
		})
}

var defaultChannelInitializer = func(config *ChannelConfig, ch *amqp.Channel) {
	log.Debug("initializing channel")

	qOptions := *config.QueueOptions
	consumer := config.Consumer

	q := createQueueOrFail(&qOptions, ch)

	if qos := config.QosOptions; qos != nil {
		setQos(qos, ch)
	}

	if config.ExchangeOptions != nil {
		exOptions := *config.ExchangeOptions
		createExchangeOrFail(&exOptions, ch)
		bindToExchange(&exOptions, ch)
	}

	qOptions.QueueName = q.Name

	if consumer != nil {

		qOptions.ConsumerName = q.Name

		msgs := createConsumerOrFail(&qOptions, ch)

		beginConsuming(msgs, func(d *amqp.Delivery) {
			config.Consumer(d)
		})
	}

}
