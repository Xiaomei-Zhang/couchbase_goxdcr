package pipeline_manager

import (
	common "github.com/Xiaomei-Zhang/couchbase_goxdcr/common"
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
	})
}

func StartPipeline(topic string) error {
	return pipeline_mgr.startPipeline(topic)
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

func (pipelineMgr *pipelineManager) startPipeline(topic string) error {
	var err error
	if f, ok := pipelineMgr.live_pipelines[topic]; !ok {
		f, err = pipelineMgr.pipeline_factory.NewPipeline(topic)
		if err != nil {
			return err
		}
		
		err = f.Start()
		if err != nil {
			return err
		}
		pipelineMgr.live_pipelines[topic] = f
	} else {
		//the pipeline is already running
		//TODO: log
	}
	return err
}

func (pipelineMgr *pipelineManager) stopPipeline(topic string) error {
	var err error
	if f, ok := pipelineMgr.live_pipelines[topic]; ok {
		f.Stop()
		delete(pipelineMgr.live_pipelines, topic)
	} else {
		//The named pipeline is not active
		//TODO: log
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

func (pipelineMgr *pipelineManager) pipelines () map[string]common.Pipeline {
	return pipelineMgr.live_pipelines
}
