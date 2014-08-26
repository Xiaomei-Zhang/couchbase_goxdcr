package test

import (
	common "github.com/Xiaomei-Zhang/couchbase_goxdcr/common"
)

type testMetricsCollector struct {
	pipeline common.Pipeline
	
	bStarted bool
	
	counter int	
}

func NewMetricsCollector () *testMetricsCollector{
	return &testMetricsCollector {nil, false, 0}
}

func (h *testMetricsCollector) Attach (pipeline common.Pipeline) error {
	h.pipeline = pipeline
	return nil
}
	
func (h *testMetricsCollector) Start(settings map[string]interface{}) error {
	h.hookup ()
	h.bStarted = true
	return nil
}

func (h *testMetricsCollector) hookup () {
	h.counter = 0
	targets := h.pipeline.Targets()
	for _, target := range targets {
		target.RegisterPartEventListener (common.DataSent, h)
	}
}

func (h *testMetricsCollector) Stop() error {
	h.cleanup()
	h.bStarted = false
	return nil
	
}

func (h *testMetricsCollector) cleanup () {
	targets := h.pipeline.Targets()
	for _, target := range targets {
		target.UnRegisterPartEventListener (common.DataSent, h)
	}
}

func (h *testMetricsCollector) OnEvent(eventType common.PartEventType, item interface{}, part common.Part, derivedItems []interface{}, otherInfos map[string]interface{}) {
	if eventType == common.DataSent {
		h.counter ++
	}
}

func (h *testMetricsCollector) MetricsValue () int {
	return h.counter
}
