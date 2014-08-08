package common

import (

)

//It is used to model the openning that data streaming out of
// the source system and the outlet where the data flowing into the target system.
//Nozzle can be opened or closed. An closed nozzle will not allow data flow through
type Nozzle interface {
	//open the Nozzle
	//data can be passed to the downstream
	Open() error
	//close the Nozzle
	//data can get to this nozzle
	//but would not be passed to the downstream
	Close() error	
	//make the nozzle working now
	Start (settings map[string]interface{} ) error
	//stop the nozzle
	Stop () error
}