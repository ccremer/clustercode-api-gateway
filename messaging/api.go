package messaging

import (
	xml2 "encoding/xml"
	"github.com/streadway/amqp"
	"net/url"
	"strconv"
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
)

func fromXml(xml string, value interface{}) error {
	if valid, err := ValidateXml(&xml); valid {
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
