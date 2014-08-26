package connector

import (
	//	"errors"
	common "github.com/Xiaomei-Zhang/couchbase_goxdcr/common"
	"log"
)

//SimpleConnector connects one source to one downstream
type SimpleConnector struct {
	downStreamPart common.Part
}

func (con *SimpleConnector) Forward(data interface{}) error {
	log.Printf("Try to forward to downstream part %s", con.downStreamPart.Id())
	return con.downStreamPart.Receive(data)
}

func (con *SimpleConnector) DownStreams() map[string]common.Part {
	downStreams := make(map[string]common.Part)
	downStreams[con.downStreamPart.Id()] = con.downStreamPart
	return downStreams
}

//add a node to its existing set of downstream nodes
func (con *SimpleConnector) AddDownStream(partId string, part common.Part) error {
	if con.downStreamPart == nil {
		con.downStreamPart = part
	} else {
		//TODO: log
		//replace the new Part with the existing one
		con.downStreamPart = part
	}
	return nil

}
