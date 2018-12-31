package messaging

import (
	"flag"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"testing"
)

var update = flag.Bool("update", false, "update .golden files")

func TestValidateSliceAddedEvent(t *testing.T) {
	xml, _ := ioutil.ReadFile("testdata/task_slice_added.xml.golden")
	xmlString := string(xml)

	if *update {
		t.Log("update golden file")
		if err := ioutil.WriteFile(gp, b.Bytes(), 0644); err != nil {
			t.Fatalf("failed to update golden file: %s", err)
		}
	}

	LoadSchema("../schema/clustercode_v1.xsd")

	err := ValidateMessage(&xmlString)
	assert.NoError(t, err)
}
