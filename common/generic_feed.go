package common

import (
	"sync"
)

//assumption here is all the processing steps are self-connected, so
//once incoming nozzle is open, it will propel the data through all
//processing steps.
type GenericFeed struct {
	topic     string
	sources   map[string]Nozzle
	endpoints map[string]Nozzle

	context FeedRuntimeContext

	// communication channel with FeedManager
	reqch chan []interface{}
	finch chan bool

	// misc.
	logPrefix string

	//if the feed is active running
	isActive bool

	//the lock to serialize the request to start\stop the feed
	stateLock sync.Mutex
}

func (genericFeed *GenericFeed) RuntimeContext() FeedRuntimeContext {
	return genericFeed.context
}


//Start the feed
//settings - a map of parameter to start the feed. it can contain initialization paramters
//			 for each processing steps, and FeedRuntime of the feed.
func (genericFeed *GenericFeed) Start(settings map[string]interface{}) error {
	var err error
	
	genericFeed.stateLock.Lock()
	defer genericFeed.stateLock.Unlock()
	
	//start all the processing steps of the Feed
	//start the incoming nozzle which would start the downstream steps
	//subsequently
	for _, source := range genericFeed.sources {
		err = source.Start(settings)
	}

	//open endpoints
	for _, endpoint := range genericFeed.endpoints {
		err = endpoint.Open()
		if err != nil {
			return err
		}
	}

	//open streams
	for _, source := range genericFeed.sources {
		err = source.Open()
		if err != nil {
			return err
		}
	}

	//start the runtime
	err = genericFeed.context.Start(settings)
	
	genericFeed.isActive = true
	
	return err
}

func (genericFeed *GenericFeed) Stop() error {
	var err error

	genericFeed.stateLock.Lock()
	defer genericFeed.stateLock.Unlock()

	for _, source := range genericFeed.sources {
		err = source.Stop()
		if err != nil {
			return err
		}
	}

	err = genericFeed.context.Stop()

	genericFeed.isActive = false
	
	return err

}

func (genericFeed *GenericFeed) Sources() map[string]Nozzle {
	return genericFeed.sources
}

func (genericFeed *GenericFeed) Targets() map[string]Nozzle {
	return genericFeed.endpoints
}

func NewGenericFeed(t string, sources map[string]Nozzle, endpoints map[string]Nozzle, r FeedRuntimeContext, reqChan chan []interface{}, finChan chan bool) *GenericFeed {
	feed := &GenericFeed{topic: t,
		sources:   sources,
		endpoints: endpoints,
		context:   r,
		reqch:     reqChan,
		finch:     finChan,
		logPrefix: "feed"}

	return feed
}
