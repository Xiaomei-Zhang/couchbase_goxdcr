package pipeline_manager

import (
	common "github.com/Xiaomei-Zhang/couchbase_goxdcr/common"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr/log"
	"sync"
)

//var logger = log.NewLogger ("PipelineManager", log.LogLevelInfo)

type pipelineManager struct {
	pipeline_factory common.PipelineFactory
	live_pipelines   map[string]common.Pipeline

	once sync.Once

	mapLock sync.Mutex
	logger  *log.CommonLogger
}

var pipeline_mgr pipelineManager

func PipelineManager(factory common.PipelineFactory, logger_context *log.LoggerContext) {
	pipeline_mgr.once.Do(func() {
		pipeline_mgr.pipeline_factory = factory
		pipeline_mgr.live_pipelines = make(map[string]common.Pipeline)
		pipeline_mgr.logger = log.NewLogger("PipelineManager", logger_context)
		pipeline_mgr.logger.Info("Pipeline Manager is constucted")
	})
}

func StartPipeline(topic string, settings map[string]interface{}) (common.Pipeline, error) {
	p, err := pipeline_mgr.startPipeline(topic, settings)
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
	pipelineMgr.logger.Infof("Starting the pipeline %s", topic)

	if f, ok := pipelineMgr.live_pipelines[topic]; !ok {
		f, err = pipelineMgr.pipeline_factory.NewPipeline(topic)
		if err != nil {
			pipelineMgr.logger.Errorf("Failed to construct a new pipeline: %s", err.Error())
			return f, err
		}

		pipelineMgr.logger.Info("Pipeline is constructed, start it")
		err = f.Start(settings)
		if err != nil {
			pipelineMgr.logger.Error("Failed to start the pipeline")
			return f, err
		}
		pipelineMgr.addPipelineToMap(f)
		return f, nil
	} else {
		//the pipeline is already running
		pipelineMgr.logger.Info("The pipeline asked to be started is already running")
		return f, err
	}
	return nil, err
}

func (pipelineMgr *pipelineManager) addPipelineToMap(p common.Pipeline) {
	pipelineMgr.mapLock.Lock()
	defer pipelineMgr.mapLock.Unlock()

	pipelineMgr.live_pipelines[p.Topic()] = p

}

func (pipelineMgr *pipelineManager) getPipelineFromMap(topic string) common.Pipeline {
	pipelineMgr.mapLock.Lock()
	defer pipelineMgr.mapLock.Unlock()

	return pipelineMgr.live_pipelines[topic]
}

func (pipelineMgr *pipelineManager) removePipelineFromMap(p common.Pipeline) {
	pipelineMgr.mapLock.Lock()
	defer pipelineMgr.mapLock.Unlock()

	delete(pipelineMgr.live_pipelines, p.Topic())
}

func (pipelineMgr *pipelineManager) stopPipeline(topic string) error {
	pipelineMgr.logger.Infof("Try to stop the pipeline %s", topic)
	var err error
	if f, ok := pipelineMgr.live_pipelines[topic]; ok {
		f.Stop()
		pipelineMgr.logger.Debug("Pipeline is stopped")
		pipelineMgr.removePipelineFromMap(f)
	} else {
		//The named pipeline is not active
		pipelineMgr.logger.Debug("The pipeline asked to be stopped is not running.")
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
	pipeline := pipelineMgr.getPipelineFromMap(topic)
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
