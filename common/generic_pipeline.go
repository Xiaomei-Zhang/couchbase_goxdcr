package common

import (
	"sync"
)

//assumption here is all the processing steps are self-connected, so
//once incoming nozzle is open, it will propel the data through all
//processing steps.
type GenericPipeline struct {
	topic     string
	sources   map[string]Nozzle
	endpoints map[string]Nozzle

	context PipelineRuntimeContext

	// communication channel with PipelineManager
	reqch chan []interface{}
	finch chan bool

	// misc.
	logPrefix string

	//if the pipeline is active running
	isActive bool

	//the lock to serialize the request to start\stop the pipeline
	stateLock sync.Mutex
}

func (genericPipeline *GenericPipeline) RuntimeContext() PipelineRuntimeContext {
	return genericPipeline.context
}


//Start the pipeline
//settings - a map of parameter to start the pipeline. it can contain initialization paramters
//			 for each processing steps, and PipelineRuntime of the pipeline.
func (genericPipeline *GenericPipeline) Start(settings map[string]interface{}) error {
	var err error
	
	genericPipeline.stateLock.Lock()
	defer genericPipeline.stateLock.Unlock()
	
	//start all the processing steps of the Pipeline
	//start the incoming nozzle which would start the downstream steps
	//subsequently
	for _, source := range genericPipeline.sources {
		err = source.Start(settings)
	}

	//open endpoints
	for _, endpoint := range genericPipeline.endpoints {
		err = endpoint.Open()
		if err != nil {
			return err
		}
	}

	//open streams
	for _, source := range genericPipeline.sources {
		err = source.Open()
		if err != nil {
			return err
		}
	}

	//start the runtime
	err = genericPipeline.context.Start(settings)
	
	genericPipeline.isActive = true
	
	return err
}

func (genericPipeline *GenericPipeline) Stop() error {
	var err error

	genericPipeline.stateLock.Lock()
	defer genericPipeline.stateLock.Unlock()

	for _, source := range genericPipeline.sources {
		err = source.Stop()
		if err != nil {
			return err
		}
	}

	err = genericPipeline.context.Stop()

	genericPipeline.isActive = false
	
	return err

}

func (genericPipeline *GenericPipeline) Sources() map[string]Nozzle {
	return genericPipeline.sources
}

func (genericPipeline *GenericPipeline) Targets() map[string]Nozzle {
	return genericPipeline.endpoints
}

func NewGenericPipeline(t string, sources map[string]Nozzle, endpoints map[string]Nozzle, r PipelineRuntimeContext, reqChan chan []interface{}, finChan chan bool) *GenericPipeline {
	pipeline := &GenericPipeline{topic: t,
		sources:   sources,
		endpoints: endpoints,
		context:   r,
		reqch:     reqChan,
		finch:     finChan,
		logPrefix: "pipeline"}

	return pipeline
}
