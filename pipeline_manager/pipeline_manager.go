package pipeline_manager

import (
	common "github.com/Xiaomei-Zhang/couchbase_goxdcr/common"
	"log"
	"sync"
)

type pipelineManager struct {
	pipeline_factory common.PipelineFactory
	live_pipelines   map[string]common.Pipeline

	once sync.Once
}

var pipeline_mgr pipelineManager

func PipelineManager(factory common.PipelineFactory) {
	pipeline_mgr.once.Do(func() {
		pipeline_mgr.pipeline_factory = factory
		pipeline_mgr.live_pipelines = make (map[string]common.Pipeline)
		log.Println("Pipeline Manager is constucted")
	})
}

func StartPipeline(topic string, settings map[string]interface{}) (common.Pipeline, error) {
	p, err := pipeline_mgr.startPipeline(topic, settings)
	log.Println("RETURN !!!!")
	return p, err
}

func StopPipeline(topic string) error {
	return pipeline_mgr.stopPipeline(topic)
}

func Pipeline(topic string) common.Pipeline {
	return pipeline_mgr.pipeline(topic)
}

func Topics() []string {
	return pipeline_mgr.topics()
}

func Pipelines() map[string]common.Pipeline {
	return pipeline_mgr.pipelines()
}

func RuntimeCtx(topic string) common.PipelineRuntimeContext {
	return pipeline_mgr.runtimeCtx(topic)
}

func (pipelineMgr *pipelineManager) startPipeline(topic string, settings map[string]interface{}) (common.Pipeline, error) {
	var err error
	log.Println("starting the pipeline " + topic)

	if f, ok := pipelineMgr.live_pipelines[topic]; !ok {
		f, err = pipelineMgr.pipeline_factory.NewPipeline(topic)
		if err != nil {
			log.Println("Failed to construct a new pipeline: " + err.Error())
			return f, err
		}

		log.Println("Pipeline is constructed, start it")
		err = f.Start(settings)
		if err != nil {
			log.Println("Failed to start the pipeline")
			return f, err
		}
		pipelineMgr.live_pipelines[topic] = f
		log.Println("RETURN from startPipeline...")
		return f, nil
	} else {
		//the pipeline is already running
		log.Println("The pipeline asked to be started is already running")
		return f, err
	}
	return nil, err
}

func (pipelineMgr *pipelineManager) stopPipeline(topic string) error {
	log.Println("Try to stop the pipeline " + topic)
	var err error
	if f, ok := pipelineMgr.live_pipelines[topic]; ok {
		f.Stop()
		log.Println("Pipeline is stopped")
		delete(pipelineMgr.live_pipelines, topic)
	} else {
		//The named pipeline is not active
		log.Println("The pipeline asked to be stopped is not running.")
	}
	return err
}

func (pipelineMgr *pipelineManager) runtimeCtx(topic string) common.PipelineRuntimeContext {
	pipeline := pipelineMgr.live_pipelines[topic]
	if pipeline != nil {
		return pipeline.RuntimeContext()
	}

	return nil
}

func (pipelineMgr *pipelineManager) pipeline(topic string) common.Pipeline {
	pipeline := pipelineMgr.live_pipelines[topic]
	return pipeline
}

func (pipelineMgr *pipelineManager) topics() []string {
	topics := make([]string, 0, len(pipelineMgr.live_pipelines))
	for k := range pipelineMgr.live_pipelines {
		topics = append(topics, k)
	}
	return topics
}

func (pipelineMgr *pipelineManager) pipelines() map[string]common.Pipeline {
	return pipelineMgr.live_pipelines
}
