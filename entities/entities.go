package entities

import (
	json2 "encoding/json"
	xml2 "encoding/xml"
	"github.com/ccremer/clustercode-api-gateway/messaging"
	"github.com/ccremer/clustercode-api-gateway/schema"
	"github.com/micro/go-config"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"net/url"
	"strconv"
	"time"
)

const (
	Complete             CompletionType = 0
	Incomplete           CompletionType = 1
	IncompleteAndRequeue CompletionType = 2
	StdInFileDescriptor                 = 0
	StdOutFileDescriptor                = 1
	StdErrFileDescriptor                = 2
)

type (
	CompletionType int
	TaskAddedEvent struct {
		JobID     string `xml:"JobId"`
		File      *url.URL
		SliceSize int
		FileHash  string
		Args      []string `xml:"Args>Arg,omitempty"`
		delivery  *amqp.Delivery
	}
	TaskCompletedEvent struct {
		JobID string `xml:"JobId"`
	}
	TaskCancelledEvent struct {
		JobID    string `xml:"JobId"`
		delivery *amqp.Delivery
	}
	SliceAddedEvent struct {
		JobID    string `xml:"JobId"`
		SliceNr  int
		Args     []string `xml:"Args>Arg,omitempty"`
		delivery *amqp.Delivery
	}
	SliceCompletedEvent struct {
		JobID      string `xml:"JobId"`
		FileHash   string `xml:",omitempty"`
		SliceNr    int
		StdStreams []StdStream `xml:"StdStreams>L,omitempty"`
	}
	StdStream struct {
		FD   int    `xml:"fd,attr"`
		Line string `xml:",innerxml"`
	}
	Message interface {
		SetComplete(completionType CompletionType)
	}
)

func DeserializeSliceAddedEvent(d *amqp.Delivery) (*SliceAddedEvent, error) {
	event := &SliceAddedEvent{
		delivery: d,
	}
	if err := FromXml(string(d.Body), event); err != nil {
		return nil, err
	}
	return event, nil
}

func DeserializeTaskCancelledEvent(d *amqp.Delivery) (*TaskCancelledEvent, error) {
	event := &TaskCancelledEvent{
		delivery: d,
	}
	if err := FromXml(string(d.Body), event); err != nil {
		return nil, err
	}
	return event, nil
}

func DeserializeTaskAddedEvent(d *amqp.Delivery) (*TaskAddedEvent, error) {
	event := &TaskAddedEvent{
		delivery: d,
	}
	if err := FromXml(string(d.Body), event); err != nil {
		return nil, err
	}
	return event, nil
}

var Validator *schema.Validator

func FromJson(json string, value interface{}) error {
	arr := []byte(json)
	return json2.Unmarshal(arr, &value)
}

func ToJson(value interface{}) (string, error) {
	json, err := json2.Marshal(&value)
	if err == nil {
		return string(json[:]), nil
	} else {
		return "", err
	}
}

func FromXml(xml string, value interface{}) error {
	if valid, err := Validator.ValidateXml(&xml); valid {
		arr := []byte(xml)
		err := xml2.Unmarshal(arr, &value)
		return err
	} else {
		return err
	}
}

func ToXml(value interface{}) (string, error) {
	xml, err := xml2.Marshal(&value)
	if err == nil {
		return string(xml[:]), nil
	} else {
		return "", err
	}
}

func (e TaskCancelledEvent) SetComplete(completionType CompletionType) {
	acknowledgeMessage(completionType, e.delivery)
}

func (e SliceAddedEvent) SetComplete(completionType CompletionType) {
	acknowledgeMessage(completionType, e.delivery)
}

func (e TaskAddedEvent) SetComplete(completionType CompletionType) {
	acknowledgeMessage(completionType, e.delivery)
}

func (e TaskAddedEvent) Priority() int {
	port, err := strconv.Atoi(e.File.Port())
	if err == nil {
		return port
	} else {
		return 0
	}
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
		log.WithField("type", completionType).Panic("type is not expected here")
	}
}

func failOnDeserialize(err error, payload []byte) {
	if err != nil {
		log.WithFields(log.Fields{
			"payload": string(payload),
			"error":   err,
			"help":    "Try purging the invalid messages (they have not been ack'ed) and try again",
		}).Fatal("could not deserialize message.")
	}
}

func Initialize() {
	log.Info("Called initilaize")

	serverUrl := config.Get("rabbitmq", "url").String("amqp://guest:guest@rabbitmq:5672/")
	service := messaging.NewRabbitMqService(serverUrl)

	qConfig := messaging.NewQueueOptions()
	eConfig := messaging.NewExchangeOptions()
	taskCancelledConfig := &messaging.ChannelConfig{
		ExchangeOptions: eConfig,
		QueueOptions:    qConfig,
		Consumer: func(d *amqp.Delivery) {
			xml := string(d.Body)
			if _, err := Validator.ValidateXml(&xml); err != nil {
				log.WithField("payload", xml).Warn("message is not valid XML")
				return
			}
			event := TaskCancelledEvent{}
			err := FromXml(string(d.Body), &event)
			failOnDeserialize(err, d.Body)
			event.delivery = d
			log.Info(event)
		}}

	LoadOptionsFromConfigOrFail(qConfig, "rabbitmq", "channels", "task", "cancelled", "queue")
	LoadOptionsFromConfigOrFail(eConfig, "rabbitmq", "channels", "task", "cancelled", "exchange")

	service.Start(taskCancelledConfig)
	//service.AddChannelConfig(taskCancelledConfig)

	go func() {
		for i := 0; true; i++ {
			service.Publish(taskCancelledConfig, "a"+strconv.Itoa(i))
			time.Sleep(10 * time.Second)
		}
	}()
	go func() {
		for i := 0; true; i++ {
			service.Publish(taskCancelledConfig, "b"+strconv.Itoa(i))
			time.Sleep(12 * time.Second)
		}
	}()
}

func LoadOptionsFromConfigOrFail(value interface{}, path ...string) {
	if err := config.Get(path[:]...).Scan(&value); err != nil {
		log.WithFields(log.Fields{
			"error": err,
			"help":  "You might want to double-check your RabbitMQ settings. Best keep them at default values",
			"path":  path,
		}).Fatal("could not load config section")
	}
}

func LoadChannelFromConfigOrFail(value *messaging.ChannelConfig, path ...string) {
	if err := config.Get(path[:]...).Scan(&value); err != nil {
		log.WithFields(log.Fields{
			"error": err,
			"help":  "You might want to double-check your RabbitMQ settings. Best keep them at default values",
			"path":  path,
		}).Fatal("could not load RabbitMQ channel config")
	}
}
