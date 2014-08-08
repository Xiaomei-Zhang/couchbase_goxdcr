package common

import (

)

//interface for Feed

type Feed interface {
	//Name of the Feed
	Topic() string

	Sources () map[string]Nozzle
	Targets () map[string]Nozzle
	
	//getter\setter of the runtime environment
	RuntimeContext() FeedRuntimeContext

	//start the data exchange
	Start() error
	//stop the data exchange
	Stop() error
}
