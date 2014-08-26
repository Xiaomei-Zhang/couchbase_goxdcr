package common

import (
	"fmt"
	"log"
	"sync"
)

//GenericPipeline is the generic implementation of a data processing pipeline
//
//The assumption here is all the processing steps are self-connected, so
//once incoming nozzle is open, it will propel the data through all
//processing steps.
type GenericPipeline struct {

	//name of the pipeline
	topic string

	//incoming nozzles of the pipeline
	sources map[string]Nozzle

	//outgoing nozzles of the pipeline
	targets map[string]Nozzle

	//runtime context of the pipeline
	context PipelineRuntimeContext

	//	//communication channel with PipelineManager
	//	reqch chan []interface{}

	//if the pipeline is active running
	isActive bool

	//the lock to serialize the request to start\stop the pipeline
	stateLock sync.Mutex
}

//Get the runtime context of this pipeline
func (genericPipeline *GenericPipeline) RuntimeContext() PipelineRuntimeContext {
	return genericPipeline.context
}

func (genericPipeline *GenericPipeline) SetRuntimeContext(ctx PipelineRuntimeContext) {
	genericPipeline.context = ctx
}

func (genericPipeline *GenericPipeline) startPart(part Part, settings map[string]interface{}) error {
	var err error = nil
	//start downstreams
	if part.Connector() != nil {
		downstreamParts := part.Connector().DownStreams()
		for _, p := range downstreamParts {
			err = genericPipeline.startPart(p, settings)
			if err != nil {
				return err
			}
		}
	}

	err = part.Start(settings)
	return err
}

//Start starts the pipeline
//
//settings - a map of parameter to start the pipeline. it can contain initialization paramters
//			 for each processing steps and for runtime context of the pipeline.
func (genericPipeline *GenericPipeline) Start(settings map[string]interface{}) error {
	log.Println("Try to start the pipeline with settings = " + fmt.Sprint(settings))
	var err error

	genericPipeline.stateLock.Lock()
	defer genericPipeline.stateLock.Unlock()

	log.Println("Start sources")
	//start all the processing steps of the Pipeline
	//start the incoming nozzle which would start the downstream steps
	//subsequently
	for _, source := range genericPipeline.sources {
		err = genericPipeline.startPart(source,settings)
		log.Println("Incoming nozzle " + source.Id() + " is started")
	}
	log.Println("All parts has been started")

	log.Println("Try to open all outgoing nozzles")
	//open targets
	for _, target := range genericPipeline.targets {
		err = target.Open()
		if err != nil {
			log.Println("Failed to open outgoing nozzle " + target.Id())
			return err
		}
	}
	log.Println("All outgoing nozzles have been opened")

	log.Println("Try to open all incoming nozzles")
	//open source
	for _, source := range genericPipeline.sources {
		err = source.Open()
		if err != nil {
			log.Println("Failed to open incoming nozzle " + source.Id())
			return err
		}
	}
	log.Println("All incoming nozzles have been opened")

	log.Println("Try to start the runtime context")
	//start the runtime
	err = genericPipeline.context.Start(settings)
	log.Println("The runtime context is started")

	//set its state to be active
	genericPipeline.isActive = true

	log.Printf("Pipeline %s is started\n", genericPipeline.Topic())

	return err
}

func (genericPipeline *GenericPipeline) stopPart(part Part) error {
	err := part.Stop()
	if err == nil {
		if part.Connector() != nil {
			downstreamParts := part.Connector().DownStreams()
			for _, p := range downstreamParts {
				err = genericPipeline.stopPart(p)
				if err != nil {
					return err
				}
			}
		}
	}
	return err
}

//Stop stops the pipeline
func (genericPipeline *GenericPipeline) Stop() error {
	var err error

	genericPipeline.stateLock.Lock()
	defer genericPipeline.stateLock.Unlock()

	//close the sources
	for _, source := range genericPipeline.sources {
		err = source.Close()
		if err != nil {
			return err
		}
	}
	log.Println("Incoming nozzles are closed, preparing to stop.")

	//stop the sources
	//source nozzle would notify the stop intention to its downsteam steps
	for _, source := range genericPipeline.sources {
		err = genericPipeline.stopPart (source)
		if err != nil {
			return err
		}
	}

	//stop runtime context only if all the processing steps in the pipeline
	//has been stopped.
	finchan := make(chan bool)
	go genericPipeline.waitToStop(finchan)
	<-finchan

	err = genericPipeline.context.Stop()

	genericPipeline.isActive = false

	return err

}

func (genericPipeline *GenericPipeline) Sources() map[string]Nozzle {
	return genericPipeline.sources
}

func (genericPipeline *GenericPipeline) Targets() map[string]Nozzle {
	return genericPipeline.targets
}

func (genericPipeline *GenericPipeline) Topic() string {
	return genericPipeline.topic
}

func (genericPipeline *GenericPipeline) waitToStop(finchan chan bool) {
	done := true
	for {
		for _, target := range genericPipeline.targets {
			if target.IsStarted() {
				done = false
			}
		}
		if done {
			break
		}
	}
	finchan <- true
}

func NewGenericPipeline(t string, sources map[string]Nozzle, targets map[string]Nozzle) *GenericPipeline {
	pipeline := &GenericPipeline{topic: t,
		sources:  sources,
		targets:  targets,
		isActive: false}

	return pipeline
}

func GetAllParts(p Pipeline) map[string]Part {
	parts := make(map[string]Part)
	sources := p.Sources()
	for key, source := range sources {
		parts[key] = source

		addDownStreams(source, parts)
	}

	return parts
}

func addDownStreams(p Part, partsMap map[string]Part) {
	connector := p.Connector()
	if connector != nil {
		downstreams := connector.DownStreams()
		for key, part := range downstreams {
			partsMap[key] = part

			addDownStreams(part, partsMap)
		}
	}
}

//enforcer for GenericPipeline to implement Pipeline
var _ Pipeline = (*GenericPipeline)(nil)
