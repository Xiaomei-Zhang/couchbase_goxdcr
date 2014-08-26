package part

import (
	"fmt"
	common "github.com/Xiaomei-Zhang/couchbase_goxdcr/common"
	"log"
	"errors"
)

type AbstractPart struct {
	connector common.Connector
	id        string

	event_listeners map[common.PartEventType][]common.PartEventListener
	//	dataChan chan interface{}
	//	communicationChan chan []interface{}

}

func NewAbstractPart(id string) AbstractPart {
	return AbstractPart{
		id:              id,
		connector:       nil,
		event_listeners: make(map[common.PartEventType][]common.PartEventListener),
	}
}

func (p *AbstractPart) RaiseEvent(eventType common.PartEventType, data interface{}, part common.Part, derivedData []interface{}, otherInfos map[string]interface{}) {

	log.Printf("Raise event %d for part %s\n", eventType, part.Id())
	listenerList := p.event_listeners[eventType]

	for _, listener := range listenerList {
		if listener != nil {
			listener.OnEvent(eventType, data, part, derivedData, otherInfos)
		}
	}
}

func (p *AbstractPart) Connector() common.Connector {
	return p.connector
}

func (p *AbstractPart) SetConnector(connector common.Connector) error {
	p.connector = connector
	return nil
}

func (p *AbstractPart) Id() string {
	return p.id
}

func (p *AbstractPart) RegisterPartEventListener(eventType common.PartEventType, listener common.PartEventListener) error {
	listenerList := p.event_listeners[eventType]
	if listenerList == nil {
		listenerList = make([]common.PartEventListener, 15)
	}

	listenerList = append(listenerList, listener)
	return nil
}

func (p *AbstractPart) UnRegisterPartEventListener(eventType common.PartEventType, listener common.PartEventListener) error {
	listenerList := p.event_listeners[eventType]
	var index int = -1

	for i, l := range listenerList {
		if l == listener {
			index = i
			log.Println("listener's index is " + fmt.Sprint(i))
			break
		}
	}

	if index >= 0 {
		listenerList = append(listenerList[:index], listenerList[index+1:]...)
		p.event_listeners[eventType] = listenerList
	}else {
		return errors.New ("UnRegisterPartEventListener failed: can't find listener " + fmt.Sprint(listener))
	}
	return nil
}
