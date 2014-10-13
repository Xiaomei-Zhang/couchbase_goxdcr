package part

import (
	"errors"
	common "github.com/Xiaomei-Zhang/couchbase_goxdcr/common"
	component "github.com/Xiaomei-Zhang/couchbase_goxdcr/component"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr/log"
	"sync"
)

//var logger = log.NewLogger("AbstractPart", log.LogLevelInfo)

type IsStarted_Callback_Func func() bool

type AbstractPart struct {
	component.AbstractComponent
	connector common.Connector

	isStarted_callback *IsStarted_Callback_Func

	stateLock sync.RWMutex
	logger    *log.CommonLogger
}

func NewAbstractPartWithLogger(id string,
	isStarted_callback *IsStarted_Callback_Func,
	logger *log.CommonLogger) AbstractPart {
	return AbstractPart{
		AbstractComponent: component.NewAbstractComponentWithLogger(id, logger),
		connector:          nil,
		isStarted_callback: isStarted_callback,
	}
}

func NewAbstractPart(id string,
	isStarted_callback *IsStarted_Callback_Func) AbstractPart {
	return NewAbstractPartWithLogger(id, isStarted_callback, log.NewLogger("AbstractPart", log.DefaultLoggerContext))
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

//func (p *AbstractPart) Logger() *log.CommonLogger {
//	return p.logger
//}
