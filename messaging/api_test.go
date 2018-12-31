package messaging

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDeserializeTaskAddedEvent(t *testing.T) {
	json := string(`
        {
            "file": "0/path/to/file.ext",
            "args": [
                "arg1",
                "arg with space"
            ],
            "job_id": "620b8251-52a1-4ecd-8adc-4fb280214bba",
            "file_hash": "b8934ef001960cafc224be9f1e1ca82c",
            "priority": 1,
            "slice_size": 120
        }
        `)
	expected := TaskAddedEvent{
		Args:      []string{"arg1", "arg with space"},
		File:      "0/path/to/file.ext",
		JobID:     "620b8251-52a1-4ecd-8adc-4fb280214bba",
		Priority:  1,
		SliceSize: 120,
		FileHash:  "b8934ef001960cafc224be9f1e1ca82c",
	}

	result := TaskAddedEvent{}
	fromJson(json, &result)

	assert.Equal(t, expected, result)
}

func TestDeserializeSliceAddedEvent(t *testing.T) {
	json := string(`
        {
            "job_id": "620b8251-52a1-4ecd-8adc-4fb280214bba",
            "args": [
                "arg1",
                "arg with space"
            ],
            "slice_nr": 34
        }
        `)
	expected := SliceAddedEvent{
		Args:    []string{"arg1", "arg with space"},
		JobID:   "620b8251-52a1-4ecd-8adc-4fb280214bba",
		SliceNr: 34,
	}

	result := SliceAddedEvent{}
	fromJson(json, &result)

	assert.Equal(t, expected, result)
}

func TestDeserializeSliceCompleteEvent(t *testing.T) {
	json := string(`
        {
            "job_id": "620b8251-52a1-4ecd-8adc-4fb280214bba",
            "slice_nr": 34,
            "file_hash": "b8934ef001960cafc224be9f1e1ca82c"
        }
        `)
	expected := SliceCompletedEvent{
		JobID:    "620b8251-52a1-4ecd-8adc-4fb280214bba",
		SliceNr:  34,
		FileHash: "b8934ef001960cafc224be9f1e1ca82c",
	}

	result := SliceCompletedEvent{}
	fromJson(json, &result)

	assert.Equal(t, expected, result)
}

func TestDeserializeTaskCancelledEvent(t *testing.T) {
	json := string(`
        {
            "job_id": "620b8251-52a1-4ecd-8adc-4fb280214bba"
        }
        `)
	expected := TaskCancelledEvent{
		JobID: "620b8251-52a1-4ecd-8adc-4fb280214bba",
	}

	result := TaskCancelledEvent{}
	fromJson(json, &result)

	assert.Equal(t, expected, result)
}
