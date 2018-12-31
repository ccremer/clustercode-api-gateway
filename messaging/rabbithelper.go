package messaging

import (
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

func failOnDeserialize(err error, payload []byte) {
	log.WithFields(log.Fields{
		"payload": string(payload),
		"error":   err,
		"help":    "Try purging the invalid messages (they have not been ack'ed) and try again",
	}).Fatal("Could not deserialize message.")
}

func createChannel() *amqp.Channel {
	log.Debugf("Opening a new channel.")

	channel, err := connection.Channel()

	log.WithFields(log.Fields{
		"error": err,
	}).Fatal("Failed to open channel")
	return channel
}

func createQueue(o *queueOptions, channel *amqp.Channel) amqp.Queue {
	log.WithField("queue_name", o.queueName).Debug("Creating queue")

	q, err := channel.QueueDeclare(
		o.queueName,
		o.durable,
		o.autoDelete,
		o.exclusive,
		o.noWait,
		o.args,
	)

	log.WithFields(log.Fields{
		"queue_name": o.queueName,
		"error":      err,
	}).Fatal("Failed to create queue")
	return q
}

func createExchange(o *queueOptions, channel *amqp.Channel) {
	log.WithField("exchange_name", o.exchangeName).Debug("Creating exchange")

	err := channel.ExchangeDeclare(
		o.exchangeName,
		o.exchangeType,
		o.durable,
		o.autoDelete,
		o.internal,
		o.noWait,
		o.args)

	log.WithFields(log.Fields{
		"exchange_name": o.exchangeName,
		"error":         err,
	}).Fatal("Failed to create exchange")
}

func bindToExchange(o *queueOptions, channel *amqp.Channel) {
	log.WithFields(log.Fields{
		"queue_name":    o.queueName,
		"exchange_name": o.exchangeName,
	}).Debug("Binding queue to exchange")

	err := channel.QueueBind(
		o.queueName,
		o.routingKey,
		o.exchangeName,
		o.noWait,
		o.args)

	log.WithFields(log.Fields{
		"queue_name":    o.queueName,
		"exchange_name": o.exchangeName,
		"error":         err,
	}).Fatal("Failed to bind queue")
}

func createConsumer(o *queueOptions, channel *amqp.Channel) <-chan amqp.Delivery {
	msgs, err := channel.Consume(
		o.queueName,
		o.consumerName,
		o.autoAck,
		o.exclusive,
		o.noLocal,
		o.noWait,
		o.args,
	)

	log.WithFields(log.Fields{
		"queue_name": o.queueName,
		"error":      err,
	}).Fatal("Failed to consume queue")
	return msgs
}

func ensureOnlyOneConsumerActive(channel *amqp.Channel) {
	prefetchCount, prefetchSize, global := 1, 0, false
	err := channel.Qos(
		prefetchCount,
		prefetchSize,
		global,
	)
	log.WithField("error", err).Fatal("Failed to set QoS")
}

func beginConsuming(msgs <-chan amqp.Delivery, callback messageReceivedCallback) {
	go func(msgs <-chan amqp.Delivery) {
		for msg := range msgs {
			log.WithField("payload", msg.Body).Debug("Received message")
			callback(&msg)
		}
	}(msgs)
}

func publish(options *queueOptions, channel *amqp.Channel, payload string) {
	log.WithField("payload", payload).Debug("Sending message")

	channel.Publish(
		options.exchangeName,
		options.queueName,
		options.mandatory,
		options.immediate,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/xml",
			Body:         []byte(payload),
		})
}

func acknowledgeMessage(completionType CompletionType, delivery *amqp.Delivery) {
	switch completionType {
	case Complete:
		{
			delivery.Ack(false)
		}
	case Incomplete:
		{
			delivery.Nack(false, false)
		}
	case IncompleteAndRequeue:
		{
			delivery.Nack(false, true)
		}
	default:
		log.WithField("type", completionType).Panic("Type is not expected here")
	}
}
