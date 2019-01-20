package entities

import (
	"flag"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"net/url"
	"path/filepath"
	"testing"
)

var update = flag.Bool("update", false, "update golden files")

var serializationTests = []struct {
	name     string
	expected interface{}
	result   interface{}
	testFile string
}{
	{
		"SliceAddedEvent_WithArgs",
		&SliceAddedEvent{
			Args:    []string{"arg1", "arg with space"},
			JobID:   "620b8251-52a1-4ecd-8adc-4fb280214bba",
			SliceNr: 34,
		},
		&SliceAddedEvent{},
		"slice_added_event_1.xml",
	},
	{
		"SliceAddedEvent_WithoutArgs",
		&SliceAddedEvent{
			JobID:   "620b8251-52a1-4ecd-8adc-4fb280214bba",
			SliceNr: 34,
		},
		&SliceAddedEvent{},
		"slice_added_event_2.xml",
	},
	{
		"SliceCompletedEvent_WithStreams_OneLine",
		&SliceCompletedEvent{
			JobID: "620b8251-52a1-4ecd-8adc-4fb280214bba",
			StdStreams: []StdStream{
				{FD: StdOutFileDescriptor, Line: "This is from stdout"},
			},
		},
		&SliceCompletedEvent{},
		"slice_completed_event_1.xml",
	},
	{
		"SliceCompletedEvent_WithStreams_MultipleLines",
		&SliceCompletedEvent{
			JobID: "620b8251-52a1-4ecd-8adc-4fb280214bba",
			StdStreams: []StdStream{
				{FD: StdErrFileDescriptor, Line: "This is from stderr"},
				{FD: StdOutFileDescriptor, Line: "This is from stdout"},
			},
		},
		&SliceCompletedEvent{},
		"slice_completed_event_2.xml",
	},
	{
		"SliceCompletedEvent_WithoutStreams",
		&SliceCompletedEvent{
			JobID: "620b8251-52a1-4ecd-8adc-4fb280214bba",
		},
		&SliceCompletedEvent{},
		"slice_completed_event_3.xml",
	},
}

func TestSerializeXml(t *testing.T) {
	for _, tt := range serializationTests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join("testdata", tt.testFile)

			// serialize
			xmlString, err := ToXml(tt.expected)
			assert.NoError(t, err)

			updateGoldenFileIfNecessary(t, xmlString, path)

			// verify from existing file
			xmlBytes, err := ioutil.ReadFile(path)
			assert.NoError(t, err)
			assert.Equal(t, string(xmlBytes), xmlString+"\n")
		})
	}
}

func TestDeserializeXml(t *testing.T) {
	ConfigureMessaging()
	for _, tt := range serializationTests {
		t.Run(tt.name, func(t *testing.T) {

			// get XML
			path := filepath.Join("testdata", tt.testFile)
			rawXmlBytes, ioErr := ioutil.ReadFile(path)
			assert.NoError(t, ioErr)
			xml := string(rawXmlBytes)

			// deserialize
			xmlErr := FromXml(xml, tt.result)
			assert.NoError(t, xmlErr)

			// verify
			assert.Equal(t, tt.expected, tt.result)
		})
	}
}

func TestTaskAddedEvent_Priority_ShouldReturnPort(t *testing.T) {
	cc_url, err := url.Parse("clustercode://base_dir:12/path")
	assert.NoError(t, err)
	subject := TaskAddedEvent{File: cc_url,}

	result := subject.Priority()
	assert.Equal(t, 12, result)
}

func TestTaskAddedEvent_Priority_ShouldReturnZero(t *testing.T) {
	cc_url, err := url.Parse("clustercode://base_dir/path")
	assert.NoError(t, err)
	subject := TaskAddedEvent{File: cc_url,}

	result := subject.Priority()
	assert.Equal(t, 0, result)
}

func updateGoldenFileIfNecessary(t *testing.T, content string, path string) {
	if *update {
		t.Log("update golden file")
		if err := ioutil.WriteFile(path, []byte(content+"\n"), 0644); err != nil {
			t.Fatalf("failed to update golden file: %s", err)
		}
	}
}
