package test

import (
	common "github.com/Xiaomei-Zhang/couchbase_goxdcr/common"
	pipeline "github.com/Xiaomei-Zhang/couchbase_goxdcr/pipeline"
	log "github.com/Xiaomei-Zhang/couchbase_goxdcr/log"
)

var logger = log.NewLogger ("testErrorHandler", log.DefaultLoggerContext)

type testErrorHandler struct {
	pipeline common.Pipeline

	bStarted bool
}

func NewErrorHandler() *testErrorHandler {
	return &testErrorHandler{nil, false}
}

func (h *testErrorHandler) Attach(pipeline common.Pipeline) error {
	h.pipeline = pipeline
	return nil
}

func (h *testErrorHandler) Start(settings map[string]interface{}) error {
	h.hookup()
	h.bStarted = true
	return nil
}

func (h *testErrorHandler) hookup() {
	parts := pipeline.GetAllParts(h.pipeline)
	for _, part := range parts {
		part.RegisterComponentEventListener(common.ErrorEncountered, h)
	}
}

func (h *testErrorHandler) Stop() error {
	h.cleanup()
	h.bStarted = false
	return nil

}

func (h *testErrorHandler) cleanup() {
	parts := pipeline.GetAllParts(h.pipeline)
	for _, part := range parts {
		part.UnRegisterComponentEventListener(common.ErrorEncountered, h)
	}
}

func (h *testErrorHandler) OnEvent(eventType common.ComponentEventType, item interface{}, component common.Component, derivedItems []interface{}, otherInfos map[string]interface{}) {
	if eventType == common.ErrorEncountered {
		logger.Errorf("Error encountered when processing %b at component %s", item, component.Id())
	}
}
