package messaging

import (
	"errors"
	"github.com/google/uuid"
	"github.com/micro/go-config"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"time"
)

type (
	queueOptions struct {
		exclusive     bool
		durable       bool
		autoDelete    bool
		noWait        bool
		internal      bool
		mandatory     bool
		immediate     bool
		routingKey    string
		exchangeName  string
		exchangeType  string
		queueName     string
		args          amqp.Table
		consumerName  string
		autoAck       bool
		noLocal       bool
		correlationId string
		contentType   string
		deliveryMode  uint8
		replyTo       string
	}
	Message interface {
		SetComplete(completionType CompletionType)
	}
	messageReceivedCallback func(delivery *amqp.Delivery)
)

func newQueueOptions() queueOptions {
	return queueOptions{
		exclusive:     false,
		durable:       true,
		autoDelete:    false,
		noWait:        false,
		autoAck:       false,
		mandatory:     false,
		immediate:     false,
		queueName:     "",
		args:          nil,
		routingKey:    "",
		exchangeName:  "",
		exchangeType:  "fanout",
		internal:      false,
		noLocal:       false,
		consumerName:  "",
		correlationId: "",
		contentType:   "application/xml",
		deliveryMode:  amqp.Persistent,
		replyTo:       "",
	}
}

	log.WithField("url", urlStripped).Info("Connecting to RabbitMQ server")
	conn, err := amqp.Dial(url)
)

func Initialize() {
	log.Info("Called initilaize")
		"error": err,
}

func OpenSliceAddedQueue(callback func(msg SliceAddedEvent)) {
	options := newQueueOptions()
	options.queueName = config.Get("rabbitmq", "channels", "slice", "added").String("slice-added")
	channel := createChannelOrFail()
	q := createQueueOrFail(&options, channel)

	ensureOnlyOneConsumerActive(channel)

	options.consumerName = q.Name
	options.autoAck = false
	msgs := createConsumerOrFail(&options, channel)
	beginConsuming(msgs, func(d *amqp.Delivery) {
		event := SliceAddedEvent{}
		err := FromXml(string(d.Body), &event)
		failOnDeserialize(err, d.Body)
		event.delivery = d
		callback(event)
	})
}

func OpenTaskAddedQueue(callback func(msg TaskAddedEvent)) {
	options := newQueueOptions()
	options.queueName = config.Get("rabbitmq", "channels", "task", "added").String("task-added")
	channel := createChannel()
	q := createQueue(&options, channel)

	ensureOnlyOneConsumerActive(channel)

	options.consumerName = q.Name
	options.autoAck = false
	msgs := createConsumer(&options, channel)
	beginConsuming(msgs, func(d *amqp.Delivery) {
		event := TaskAddedEvent{}
		err := FromXml(string(d.Body), &event)
		failOnDeserialize(err, d.Body)
		event.delivery = d
		callback(event)
	})
}

func OpenSliceCompleteQueue(supplier chan SliceCompletedEvent) {
	options := newQueueOptions()
	options.queueName = config.Get("rabbitmq", "channels", "slice", "completed").String("slice-completed")
	channel := createChannel()
	q := createQueue(&options, channel)
	options.queueName = q.Name

	go func(channel *amqp.Channel, options *queueOptions) {
		for {
			msg := <-supplier
			ser, _ := ToXml(msg)
			publish(options, channel, ser)
		}
	}(channel, &options)
}

func OpenTaskCompleteQueue(supplier chan TaskCompletedEvent) {
	options := newQueueOptions()
	options.queueName = config.Get("rabbitmq", "channels", "task", "completed").String("task-completed")
	channel := createChannel()
	q := createQueue(&options, channel)
	options.queueName = q.Name
	go func(channel *amqp.Channel, options *queueOptions) {
		for {
			msg := <-supplier
			ser, _ := ToXml(msg)
			publish(options, channel, ser)
		}
	}(channel, &options)
}

func OpenTaskCancelledQueue(callback func(msg TaskCancelledEvent)) {
	channel := createChannel()
	options := newQueueOptions()

	options.exchangeName = config.Get("rabbitmq", "channels", "task", "cancelled").String("task-cancelled")
	options.autoDelete = false
	options.durable = true
	createExchange(&options, channel)

	options.queueName = ""
	options.exclusive = true
	options.durable = false
	q := createQueue(&options, channel)

	options.queueName = q.Name
	bindToExchange(&options, channel)

	msgs := createConsumer(&options, channel)

	beginConsuming(msgs, func(d *amqp.Delivery) {
		event := TaskCancelledEvent{}
		err := FromXml(string(d.Body), &event)
		failOnDeserialize(err, d.Body)
		event.delivery = d
		callback(event)
	})
}
/*
func OpenFfmpegLinePrintedQueue(supplier chan FfmpegLinePrintedEvent) {
	options := newQueueOptions()
	options.queueName = config.Get("rabbitmq", "channels", "out").String("line-out")
	channel := createChannel()
	q := createQueue(&options, channel)
	options.queueName = q.Name
	go func(channel *amqp.Channel, options *queueOptions) {
		for {
			msg := <-supplier
			json, _ := ToXml(msg)
			publish(options, channel, json)
		}
	}(channel, &options)
}
*/
