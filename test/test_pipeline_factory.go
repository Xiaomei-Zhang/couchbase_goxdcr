package test

import (
	common "github.com/Xiaomei-Zhang/couchbase_goxdcr/common"
	connector "github.com/Xiaomei-Zhang/couchbase_goxdcr/connector"
	pipeline "github.com/Xiaomei-Zhang/couchbase_goxdcr/pipeline"
	pipeline_ctx "github.com/Xiaomei-Zhang/couchbase_goxdcr/pipeline_ctx"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr/log"
)

type testPipelineFactory struct {
}

func (f *testPipelineFactory) NewPipeline(topic string) (common.Pipeline, error) {

	inNozzle1 := newInComingNozzle("incoming1")
	outNozzle1 := newOutgoingNozzle("out1")
	con1 := connector.NewSimpleConnector(nil, nil)
	part1 := newTestPart("part1")
	con1.AddDownStream("part1", part1)
	con11 := connector.NewSimpleConnector(nil, nil)
	con11.AddDownStream("out1", outNozzle1)
	inNozzle1.SetConnector(con1)
	part1.SetConnector(con11)

	inNozzle2 := newInComingNozzle("incoming2")
	outNozzle2 := newOutgoingNozzle("out2")
	con2 := connector.NewSimpleConnector(nil, nil)
	part2 := newTestPart("part2")
	con2.AddDownStream("part2", part2)
	con22 := connector.NewSimpleConnector(nil, nil)
	con22.AddDownStream("out2", outNozzle2)
	inNozzle2.SetConnector(con2)
	part2.SetConnector(con22)

	sources := make(map[string]common.Nozzle)
	targets := make(map[string]common.Nozzle)
	sources["incoming1"] = inNozzle1
	sources["incoming2"] = inNozzle2
	targets["out1"] = outNozzle1
	targets["out2"] = outNozzle2

	pipeline := pipeline.NewGenericPipeline(topic, sources, targets)

	ctx, err := pipeline_ctx.NewWithCtx(pipeline, log.DefaultLoggerContext)
	metricsCollector := NewMetricsCollector()
	
	outNozzle1.RegisterPartEventListener (common.DataSent, metricsCollector)
	outNozzle2.RegisterPartEventListener (common.DataSent, metricsCollector)
	
	ctx.RegisterService("error_handler", NewErrorHandler())
	ctx.RegisterService("counter_statistic_collector", metricsCollector)

	pipeline.SetRuntimeContext(ctx)
	return pipeline, err
}
