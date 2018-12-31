package messaging

import (
	xml2 "encoding/xml"
	"github.com/streadway/amqp"
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
		JobID     string `json:"JobId" xml:"JobId"`
		File      string
		Priority  int
		SliceSize int
		FileHash  string
		Args      []string `xml:"Args>Arg"`
		delivery  *amqp.Delivery
	}
	TaskCompletedEvent struct {
		JobID string `json:"JobId" xml:"JobId"`
	}
	TaskCancelledEvent struct {
		JobID    string `json:"JobId" xml:"JobId"`
		delivery *amqp.Delivery
	}
	SliceAddedEvent struct {
		Version  int    `xml:"version,attr,omitempty"`
		JobID    string `json:"JobId" xml:"JobId"`
		SliceNr  int
		Args     []string `xml:"Args>Arg,omitempty"`
		delivery *amqp.Delivery
	}
	SliceCompletedEvent struct {
		JobID    string `json:"JobId" xml:"JobId"`
		FileHash string
		SliceNr  int
	}
	FfmpegLinePrintedEvent struct {
		JobID   string `json:"JobId" xml:"JobId"`
		SliceNr int
		FD      int
		Line    string
		Index   int64
	}
)

func fromXml(xml string, value interface{}) error {
	err := ValidateMessage(&xml)
	if err == nil {
		arr := []byte(xml)
		err := xml2.Unmarshal(arr, &value)
		return err
	} else {
		return err
	}
}

func ToXml(value interface{}) (string, error) {
	xml, err := xml2.MarshalIndent(&value, "  ", "    ")
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
