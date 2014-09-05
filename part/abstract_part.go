package part

import (
	"errors"
	"fmt"
	common "github.com/Xiaomei-Zhang/couchbase_goxdcr/common"
	log "github.com/Xiaomei-Zhang/couchbase_goxdcr/util"
	"sync"
)

var logger = log.NewLogger("AbstractPart", log.LogLevelInfo)

type AbstractPart struct {
	connector common.Connector
	id        string

	event_listeners map[common.PartEventType][]common.PartEventListener
	listenerLock sync.Mutex
}

func NewAbstractPart(id string) AbstractPart {
	return AbstractPart{
		id:              id,
		connector:       nil,
		event_listeners: make(map[common.PartEventType][]common.PartEventListener),
	}
}

func (p *AbstractPart) RaiseEvent(eventType common.PartEventType, data interface{}, part common.Part, derivedData []interface{}, otherInfos map[string]interface{}) {
	p.listenerLock.Lock()
	defer p.listenerLock.Unlock()

	logger.Debugf("Raise event %d for part %s\n", eventType, part.Id())
	listenerList := p.event_listeners[eventType]

	for _, listener := range listenerList {
		if listener != nil {
//			logger.LogDebug("", "", fmt.Sprintf("calling listener %s on event %s on part %s", fmt.Sprint(listener), fmt.Sprint(eventType), part.Id()))
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
	p.listenerLock.Lock()
	defer p.listenerLock.Unlock()

	listenerList := p.event_listeners[eventType]
	if listenerList == nil {
		listenerList = make([]common.PartEventListener, 0, 15)
	}

	listenerList = append(listenerList, listener)
	p.event_listeners[eventType] = listenerList
	logger.Infof ("listener %s is registered on event %s for part %s", fmt.Sprint(listener), fmt.Sprint(eventType), p.Id())
	return nil
}

func (p *AbstractPart) UnRegisterPartEventListener(eventType common.PartEventType, listener common.PartEventListener) error {
	p.listenerLock.Lock()
	defer p.listenerLock.Unlock()

	listenerList := p.event_listeners[eventType]
	var index int = -1

	for i, l := range listenerList {
		if l == listener {
			index = i
			logger.Debugf("listener's index is "+fmt.Sprint(i))
			break
		}
	}

	if index >= 0 {
		listenerList = append(listenerList[:index], listenerList[index+1:]...)
		p.event_listeners[eventType] = listenerList
	} else {
		return errors.New("UnRegisterPartEventListener failed: can't find listener " + fmt.Sprint(listener))
	}
	return nil
}
