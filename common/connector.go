package common

import (

)

//Connector abstracts the logic which moves data from one processing steps to another
type Connector interface {
	Forward(data interface{}) error
	
	//get this node's down stream nodes
	DownStreams () map[string]Part
	
	//add a node to its existing set of downstream nodes
	AddDownStreamNode (partId string, part Part) error
	
}
