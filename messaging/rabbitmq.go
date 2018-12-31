package messaging

import (
	"github.com/micro/go-config"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
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
	url := config.Get("rabbitmq", "url").String("amqp://guest:guest@rabbitmq:5672/")
	log.Infof("Connecting to %s", url)
	conn, err := amqp.Dial(url)
	log.Panicf("A working connection to %[2]s is necessary: %[1]s", err, url)
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
		err := fromJson(string(d.Body), &event)
		failOnDeserialize(err)
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
		err := fromJson(string(d.Body), &event)
		failOnDeserialize(err)
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
			json, _ := ToJson(msg)
			publish(options, channel, json)
			log.Debugf("Sent message to queue %s: %s", q.Name, json)
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
			json, _ := ToJson(msg)
			publish(options, channel, json)
			log.Debugf("Sent message to queue %s: %s", q.Name, json)
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
		err := fromJson(string(d.Body), &event)
		failOnDeserialize(err)
		event.delivery = d
		callback(event)
	})
}

func OpenFfmpegLinePrintedQueue(supplier chan FfmpegLinePrintedEvent) {
	options := newQueueOptions()
	options.queueName = config.Get("rabbitmq", "channels", "out").String("line-out")
	channel := createChannel()
	q := createQueue(&options, channel)
	options.queueName = q.Name
	go func(channel *amqp.Channel, options *queueOptions) {
		for {
			msg := <-supplier
			json, _ := ToJson(msg)
			publish(options, channel, json)
		}
	}(channel, &options)
}
