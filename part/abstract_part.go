package part

import (
	"errors"
	"fmt"
	common "github.com/Xiaomei-Zhang/couchbase_goxdcr/common"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr/log"
	"sync"
)

//var logger = log.NewLogger("AbstractPart", log.LogLevelInfo)

type IsStarted_Callback_Func func() bool

type AbstractPart struct {
	connector common.Connector
	id        string

	isStarted_callback *IsStarted_Callback_Func

	event_listeners map[common.PartEventType][]common.PartEventListener

	stateLock sync.RWMutex
	logger    *log.CommonLogger
}

func NewAbstractPartWithLogger(id string,
	isStarted_callback *IsStarted_Callback_Func,
	logger *log.CommonLogger) AbstractPart {
	return AbstractPart{
		id:                 id,
		connector:          nil,
		isStarted_callback: isStarted_callback,
		event_listeners:    make(map[common.PartEventType][]common.PartEventListener),
		logger:             logger,
	}
}

func NewAbstractPart(id string,
	isStarted_callback *IsStarted_Callback_Func) AbstractPart {
	return NewAbstractPartWithLogger(id, isStarted_callback, log.NewLogger("AbstractPart", log.DefaultLoggerContext))
}

func (p *AbstractPart) RaiseEvent(eventType common.PartEventType, data interface{}, part common.Part, derivedData []interface{}, otherInfos map[string]interface{}) {

	p.logger.Debugf("Raise event %d for part %s\n", eventType, part.Id())
	listenerList := p.event_listeners[eventType]

	for _, listener := range listenerList {
		if listener != nil {
			//			logger.LogDebug("", "", fmt.Sprintf("calling listener %s on event %s on part %s", fmt.Sprint(listener), fmt.Sprint(eventType), part.Id()))
			listener.OnEvent(eventType, data, part, derivedData, otherInfos)
		}
	}
}

func (p *AbstractPart) Connector() common.Connector {
	p.stateLock.RLock()
	defer p.stateLock.RUnlock()

	return p.connector
}

func (p *AbstractPart) SetConnector(connector common.Connector) error {
	if p.isStarted_callback == nil || (*p.isStarted_callback) == nil {
		return errors.New("IsStarted() call back func has not been defined for part " + p.Id())
	}
	if (*p.isStarted_callback)() {
		return errors.New("Cannot set connector on part" + p.Id() + " since the part is still running.")
	}

	p.stateLock.Lock()
	defer p.stateLock.Unlock()

	p.connector = connector
	return nil
}

func (p *AbstractPart) Id() string {
	return p.id
}

func (p *AbstractPart) RegisterPartEventListener(eventType common.PartEventType, listener common.PartEventListener) error {

	listenerList := p.event_listeners[eventType]
	if listenerList == nil {
		listenerList = make([]common.PartEventListener, 0, 15)
	}

	listenerList = append(listenerList, listener)
	p.event_listeners[eventType] = listenerList
	p.logger.Infof("listener %s is registered on event %s for part %s", fmt.Sprint(listener), fmt.Sprint(eventType), p.Id())
	return nil
}

func (p *AbstractPart) UnRegisterPartEventListener(eventType common.PartEventType, listener common.PartEventListener) error {

	listenerList := p.event_listeners[eventType]
	var index int = -1

	for i, l := range listenerList {
		if l == listener {
			index = i
			p.logger.Debugf("listener's index is " + fmt.Sprint(i))
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

//func (p *AbstractPart) Logger() *log.CommonLogger {
//	return p.logger
//}
