package messaging

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"path/filepath"
	"testing"
)

var validationTests = []struct {
	name     string
	testFile string
	isValid  bool
}{
	{
		"ClustercodeUrl_Valid_WithoutPrio",
		"clustercode_url_1.xml",
		true,
	},
	{
		"ClustercodeUrl_Valid_WithPrio",
		"clustercode_url_2.xml",
		true,
	},
	{
		"ClustercodeUrl_Invalid_WithoutPath",
		"clustercode_url_3.xml",
		false,
	},
	{
		"ClustercodeUrl_Invalid_WithPrio_EmptyPath",
		"clustercode_url_4.xml",
		false,
	},
	{
		"ClustercodeUrl_Invalid_WithoutPrio_EmptyPath",
		"clustercode_url_5.xml",
		false,
	},
	{
		"JobId_Invalid_EmptyValue",
		"job_id_1.xml",
		false,
	},
	{
		"JobId_Invalid_InvalidValue",
		"job_id_2.xml",
		false,
	},
	{
		"JobId_Invalid_InvalidUuid",
		"job_id_3.xml",
		false,
	},
}

func TestValidation(t *testing.T) {
	LoadSchema("../schema/clustercode_v1.xsd")
	for _, tt := range validationTests {
		t.Run(tt.name, func(t *testing.T) {

			// get XML
			path := filepath.Join("testdata", "validation", tt.testFile)
			rawXmlBytes, ioErr := ioutil.ReadFile(path)
			assert.NoError(t, ioErr)
			xml := string(rawXmlBytes)

			valid, err := ValidateXml(&xml)
			if tt.isValid {
				assert.NoError(t, err)
				assert.True(t, valid)
			} else {
				assert.NotEmpty(t, err)
				assert.False(t, valid)
			}
		})
	}
}
