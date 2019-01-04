package messaging

import (
	"github.com/micro/go-config"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	url2 "net/url"
)

var connection *amqp.Connection

type (
	queueOptions struct {
		exclusive    bool
		durable      bool
		autoDelete   bool
		noWait       bool
		internal     bool
		mandatory    bool
		immediate    bool
		routingKey   string
		exchangeName string
		exchangeType string
		queueName    string
		args         amqp.Table
		consumerName string
		autoAck      bool
		noLocal      bool
	}
	Message interface {
		SetComplete(completionType CompletionType)
	}
	messageReceivedCallback func(delivery *amqp.Delivery)
)

func newQueueOptions() queueOptions {
	return queueOptions{
		exclusive:    false,
		durable:      true,
		autoDelete:   false,
		noWait:       false,
		autoAck:      false,
		mandatory:    false,
		immediate:    false,
		queueName:    "",
		args:         nil,
		routingKey:   "",
		exchangeName: "",
		exchangeType: "fanout",
		internal:     false,
		noLocal:      false,
		consumerName: "",
	}
}

func Connect() *amqp.Connection {
	if connection != nil {
		return connection
	}

	// we don't want to log the credentials
	url := config.Get("rabbitmq", "url").String("amqp://guest:guest@rabbitmq:5672/")
	urlParsed, err := url2.ParseRequestURI(url)
	if err != nil {
		log.Fatal(err)
	}
	urlStripped := urlParsed.Scheme+"://"+urlParsed.Host+urlParsed.Path

	log.WithField("url", urlStripped).Info("Connecting to RabbitMQ server")
	conn, err := amqp.Dial(url)

	log.WithFields(log.Fields{
		"url": urlStripped,
		"error": err,
		"help": "Credentials have been removed from URL in the log",
	}).Fatal("Could not connect to RabbitMQ server")
	connection = conn
	return connection
}

func OpenSliceAddedQueue(callback func(msg SliceAddedEvent)) {
	options := newQueueOptions()
	options.queueName = config.Get("rabbitmq", "channels", "slice", "added").String("slice-added")
	channel := createChannel()
	q := createQueue(&options, channel)

	ensureOnlyOneConsumerActive(channel)

	options.consumerName = q.Name
	options.autoAck = false
	msgs := createConsumer(&options, channel)
	beginConsuming(msgs, func(d *amqp.Delivery) {
		event := SliceAddedEvent{}
		err := fromXml(string(d.Body), &event)
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
		err := fromXml(string(d.Body), &event)
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
		err := fromXml(string(d.Body), &event)
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
