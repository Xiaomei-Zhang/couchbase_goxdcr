package Component

import (
	"errors"
	"fmt"
	common "github.com/Xiaomei-Zhang/couchbase_goxdcr/common"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr/log"
)

type AbstractComponent struct {
	id        string
	event_listeners map[common.ComponentEventType][]common.ComponentEventListener
	logger    *log.CommonLogger
}

func NewAbstractComponentWithLogger(id string, logger *log.CommonLogger) AbstractComponent {
	return AbstractComponent{
		id:                 id,
		event_listeners:    make(map[common.ComponentEventType][]common.ComponentEventListener),
		logger:             logger,
	}
}

func NewAbstractComponent(id string) AbstractComponent {
	return NewAbstractComponentWithLogger(id, log.NewLogger("AbstractComponent", log.DefaultLoggerContext))
}

func (c *AbstractComponent) Id() string {
	return c.id
}

func (c *AbstractComponent) RegisterComponentEventListener(eventType common.ComponentEventType, listener common.ComponentEventListener) error {

	listenerList := c.event_listeners[eventType]
	if listenerList == nil {
		listenerList = make([]common.ComponentEventListener, 0, 15)
	}

	listenerList = append(listenerList, listener)
	c.event_listeners[eventType] = listenerList
	c.logger.Infof("listener %s is registered on event %s for Component %s", fmt.Sprint(listener), fmt.Sprint(eventType), c.Id())
	return nil
}

func (c *AbstractComponent) UnRegisterComponentEventListener(eventType common.ComponentEventType, listener common.ComponentEventListener) error {

	listenerList := c.event_listeners[eventType]
	var index int = -1

	for i, l := range listenerList {
		if l == listener {
			index = i
			c.logger.Debugf("listener's index is " + fmt.Sprint(i))
			break
		}
	}

	if index >= 0 {
		listenerList = append(listenerList[:index], listenerList[index+1:]...)
		c.event_listeners[eventType] = listenerList
	} else {
		return errors.New("UnRegisterComponentEventListener failed: can't find listener " + fmt.Sprint(listener))
	}
	return nil
}

func (c *AbstractComponent) RaiseEvent(eventType common.ComponentEventType, data interface{}, component common.Component, derivedData []interface{}, otherInfos map[string]interface{}) {

	c.logger.Debugf("Raise event %d for component %s\n", eventType, component.Id())
	listenerList := c.event_listeners[eventType]

	for _, listener := range listenerList {
		if listener != nil {
			//			logger.LogDebug("", "", fmt.Sprintf("calling listener %s on event %s on part %s", fmt.Sprint(listener), fmt.Sprint(eventType), part.Id()))
			listener.OnEvent(eventType, data, component, derivedData, otherInfos)
		}
	}
}


func (c *AbstractComponent) Logger() *log.CommonLogger {
	return c.logger
}
