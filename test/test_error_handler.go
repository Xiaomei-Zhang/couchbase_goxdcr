package test

import (
	common "github.com/Xiaomei-Zhang/couchbase_goxdcr/common"
	"log"
)

type testErrorHandler struct {
	pipeline common.Pipeline
	
	bStarted bool	
}

func NewErrorHandler () *testErrorHandler{
	return &testErrorHandler {nil, false}
}

func (h *testErrorHandler) Attach (pipeline common.Pipeline) error {
	h.pipeline = pipeline
	return nil
}
	
func (h *testErrorHandler) Start(settings map[string]interface{}) error {
	h.hookup ()
	h.bStarted = true
	return nil
}

func (h *testErrorHandler) hookup () {
	parts := common.GetAllParts (h.pipeline)
	for _, part := range parts {
		part.RegisterPartEventListener (common.ErrorEncountered, h)
	}
}

func (h *testErrorHandler) Stop() error {
	h.cleanup()
	h.bStarted = false
	return nil
	
}

func (h *testErrorHandler) cleanup () {
	parts := common.GetAllParts (h.pipeline)
	for _, part := range parts {
		part.UnRegisterPartEventListener (common.ErrorEncountered, h)
	}
}

func (h *testErrorHandler) OnEvent(eventType common.PartEventType, item interface{}, part common.Part, derivedItems []interface{}, otherInfos map[string]interface{}) {
	if eventType == common.ErrorEncountered {
		log.Printf("Error encountered when processing %b at part %s\n", item, part.Id())
	}
}
