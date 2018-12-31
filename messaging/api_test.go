package messaging

import (
	"flag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"io/ioutil"
	"path/filepath"
	"testing"
)

var update = flag.Bool("update", false, "update golden files")

type ApiTestSuite struct {
	suite.Suite
}

func (s *ApiTestSuite) SetupTest() {
	LoadSchema("../schema/clustercode_v1.xsd")
}

func (s *ApiTestSuite) TestDeserializeJsonTaskAddedEvent() {
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

	assert.Equal(s.T(), expected, result)
}
/*
func (s *ApiTestSuite) TestDeserializeXmlTaskAddedEvent() {
	json := string(`
        {
            "file": "${base_dir}/0/path/to/file.ext",
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
		File:      "${base_dir}/path/to/file.ext",
		JobID:     "620b8251-52a1-4ecd-8adc-4fb280214bba",
		Priority:  1,
		SliceSize: 120,
		FileHash:  "b8934ef001960cafc224be9f1e1ca82c",
	}

	result := TaskAddedEvent{}
	fromJson(json, &result)

	assert.Equal(s.T(), expected, result)
}*/

func (s *ApiTestSuite) TestDeserializeJsonSliceAddedEvent() {
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

	assert.Equal(s.T(), expected, result)
}

func (s *ApiTestSuite) TestDeserializeXmlSliceAddedEvent() {
	path := filepath.Join("testdata", "task_slice_added_1"+".xml")
	xml, err := ioutil.ReadFile(path)
	assert.NoError(s.T(), err)

	expected := SliceAddedEvent{
		Args:    []string{"arg1", "arg with space"},
		JobID:   "620b8251-52a1-4ecd-8adc-4fb280214bba",
		SliceNr: 34,
	}

	result := SliceAddedEvent{}
	xmlError := fromXml(string(xml), &result)
	assert.NoError(s.T(), xmlError)

	assert.Equal(s.T(), expected, result)
}

func (s *ApiTestSuite) TestSerializeXmlSliceAddedEvent() {
	path := filepath.Join("testdata", "task_slice_added_1"+".xml")

	value := SliceAddedEvent{
		Args:    []string{"arg1", "arg with space"},
		JobID:   "620b8251-52a1-4ecd-8adc-4fb280214bba",
		SliceNr: 34,
	}

	result, err := ToXml(value)
	updateGoldenFileIfNecessary(s, result, path)

	expected, err := ioutil.ReadFile(path)
	assert.NoError(s.T(), err)

	assert.Equal(s.T(), expected, result)
}

func (s *ApiTestSuite) TestDeserializeSliceCompleteEvent() {
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

	assert.Equal(s.T(), expected, result)
}

func (s *ApiTestSuite) TestDeserializeTaskCancelledEvent() {
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

	assert.Equal(s.T(), expected, result)
}

func TestApiTestSuite(t *testing.T) {
	suite.Run(t, new(ApiTestSuite))
}

func updateGoldenFileIfNecessary(s *ApiTestSuite, content string, path string) {
	if *update {
		s.T().Log("update golden file")
		if err := ioutil.WriteFile(path, []byte(content), 0644); err != nil {
			s.T().Fatalf("failed to update golden file: %s", err)
		}
	}
}
