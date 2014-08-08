package common

import (

)

//FeedService can be any component that monitors, does logging, keeps state for the feed
//Each FeedService is a goroutine that run parallelly
type FeedService interface {
	Attach (feed Feed)
	
	Start(map[string]interface{}) error
	Stop() error
}
