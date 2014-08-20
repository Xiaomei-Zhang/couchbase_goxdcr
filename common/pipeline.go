package common

import (

)

//interface for Pipeline

type Pipeline interface {
	//Name of the Pipeline
	Topic() string

	Sources () map[string]Nozzle
	Targets () map[string]Nozzle
	
	//getter\setter of the runtime environment
	RuntimeContext() PipelineRuntimeContext

	//start the data exchange
	Start() error
	//stop the data exchange
	Stop() error
}
