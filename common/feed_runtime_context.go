package common

import (

)

//FeedRuntimeContext manages all the services that attaches to the feed
type FeedRuntimeContext interface {

	Start(map[string]interface{}) error
	Stop() error
	
	Feed () Feed
	
	//return a service handle
	Service (srv_name string) FeedService
	
	//register a new service
	//if the feed is active, the service would be started rightway
	RegisterService (svc FeedService) error
	
	//attach the service from the feed and stop the service's goroutine
	UnregisterService (srv_name string) error
	
}

