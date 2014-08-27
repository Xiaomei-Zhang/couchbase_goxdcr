package test

import (
	common "github.com/Xiaomei-Zhang/couchbase_goxdcr/common"
	pipeline "github.com/Xiaomei-Zhang/couchbase_goxdcr/pipeline"
	log "github.com/Xiaomei-Zhang/couchbase_goxdcr/util"
)

var logger = log.NewLogger ("testErrorHandler", log.LogLevelInfo)

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
		part.RegisterPartEventListener(common.ErrorEncountered, h)
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
		part.UnRegisterPartEventListener(common.ErrorEncountered, h)
	}
}

func (h *testErrorHandler) OnEvent(eventType common.PartEventType, item interface{}, part common.Part, derivedItems []interface{}, otherInfos map[string]interface{}) {
	if eventType == common.ErrorEncountered {
		logger.Errorf("Error encountered when processing %b at part %s", item, part.Id())
	}
}
