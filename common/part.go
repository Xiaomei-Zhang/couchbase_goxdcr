package common

import (

)

//PartEventType is the common event type that Part can raise during its lifecycle
//It is not required for Part to raise all those event
type PartEventType int
const (
	DataReceived PartEventType = iota
	DataProcessed PartEventType = iota
	DataSent PartEventType = iota
	ErrorEncountered PartEventType = iota
)

//PartEventListener abstracts anybody who interests in an event of a Part
type PartEventListener interface {
	//OnEvent is the callback function that Part would notify listener on an event
	//event - the type of part event
	//item - the data item
	//derivedItems - the data items derived from the original item. This only used by DataProcessed event
	//otherinformation - any other information the event might be able to supply to its listener
	OnEvent (eventType PartEventType, item interface{}, part Part, derivedItems []interface{}, otherInfos map[string]interface{}) bool
}

type Part interface {
	Connectable
	
	//each node is uniquely identified by Id within the plan
	Id () string
	
	//Start makes goroutine for the nozzle working, it is also responsible for 
	//starting its downstream steps
	Start (settings map[string]interface{} ) error
	
	//Stop stops the nozzle,
	//it is also reponsible for sending stop request to its downstream steps
	//right before it is ready to shut down itself.
	//Downstream would stop itself when it is done with the processing of the data it has
	Stop () error

	//RegisterPartEventListener registers a listener for Part event
	//
	//if the eventType is not supported by the part, an error would be thrown
	RegisterPartEventListener (eventType PartEventType, listener PartEventListener) error
	
	
}