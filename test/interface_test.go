package test

import (
	pipeline_manager "github.com/Xiaomei-Zhang/couchbase_goxdcr/pipeline_manager"
	"log"
	"testing"
	"time"
)

func TestPipeline(t *testing.T) {
	settings := make(map[string]interface{})
	settings["start_int"] = 0
	settings["increase_amount"] = 2
	pipeline_manager.PipelineManager(&testPipelineFactory{})
	p, err := pipeline_manager.StartPipeline("ABC", settings)
	log.Println("Here---")

	if err != nil {
		t.Error("Failed to start pipeline ABC")
		t.FailNow()
	}
	log.Println("Done with starting pipeline")

	ctx := p.RuntimeContext()
	svc := ctx.Service("counter_statistic_collector")
	if svc == nil {
		t.Error("counter_statistic_collector is not a registed service on pipeline runtime")
		t.FailNow()
	}

	log.Println("Start ticking...")
	ticker :=time.NewTicker(500 * time.Millisecond)
	tickCount := 0
	go func () {
	for now := range ticker.C {
		count := svc.(*testMetricsCollector).MetricsValue
		log.Printf("%s -- %d data is processed\n", now.String(), count)
		tickCount++
		log.Printf("tickCount is %d\n", tickCount)

	}
	}()

   time.Sleep(time.Second * 1)

	log.Println("About to stop ABC")
	pipeline_manager.StopPipeline("ABC")
    ticker.Stop()

	log.Println("Succeed")
}
