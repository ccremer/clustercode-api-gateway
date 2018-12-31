package messaging

import (
	"errors"
	"fmt"
	"github.com/jbussdieker/golibxml"
	"github.com/krolaw/xsd"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"unsafe"
)

var schema *xsd.Schema

func LoadSchema(path string) {
	log.WithField("path", path).Debug("Loading schema")
	xsdfile, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}
	xsdSchema, err := xsd.ParseSchema(xsdfile)
	if err != nil {
		log.Fatal(err)
	}
	schema = xsdSchema
}

func ValidateMessage(xml *string) error {
	if schema == nil {
		log.Fatal("schema is not loaded")
	}
	doc := golibxml.ParseDoc(*xml)
	if doc == nil {
		return errors.New("provided XML string does not seem to be valid XML")
	}
	defer doc.Free()

	// golibxml._Ctype_xmlDocPtr can't be cast to xsd.DocPtr, even though they are both
	// essentially _Ctype_xmlDocPtr.  Using unsafe gets around this.
	if err := schema.Validate(xsd.DocPtr(unsafe.Pointer(doc.Ptr))); err != nil {
		return errors.New(fmt.Sprintln(err))
	} else {
		return nil
	}
}
