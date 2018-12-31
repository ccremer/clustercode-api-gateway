package messaging

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"testing"
)

func TestValidateSliceAddedEvent(t *testing.T) {
	xml, _ := ioutil.ReadFile("testdata/task_slice_added.xml.golden")
	xmlString := string(xml)



	LoadSchema("../schema/clustercode_v1.xsd")

	err := ValidateMessage(&xmlString)
	assert.NoError(t, err)
}
