package test

import (
	"errors"
	common "github.com/Xiaomei-Zhang/couchbase_goxdcr/common"
	part "github.com/Xiaomei-Zhang/couchbase_goxdcr/part"
	"log"
	"reflect"
	"sync"
)

//constants
var cmdStop = 0
var cmdHeartBeat = 1

type testPart struct {
	part.AbstractPart
	//settings
	increase_amount int

	dataChan chan interface{}
	//communication channel
	communicationChan chan []interface{}

	isStarted bool

	//the lock to serialize the request to start\stop the nozzle
	stateLock sync.Mutex
	
	waitGrp sync.WaitGroup
}

func newTestPart(id string) *testPart {
	abstractPart := part.NewAbstractPart(id)
	p := &testPart{abstractPart, 0, nil, nil, false, sync.Mutex{}, sync.WaitGroup{}}
	return p
}

func (p *testPart) Start(settings map[string]interface{}) error {
	log.Printf("Try to start part %s\n", p.Id())
	p.stateLock.Lock()
	defer p.stateLock.Unlock()

	p.init(settings)
	go p.run()
	p.isStarted = true
	return nil
}

func (p *testPart) init(settings map[string]interface{}) error {
	p.dataChan = make(chan interface{}, 1)
	p.communicationChan = make(chan []interface{}, 1)

	amt, ok := settings["increase_amount"]
	if ok {
		if reflect.TypeOf(amt).Name() != "int" {
			return errors.New("Wrong parameter. Expects int as the value for 'increase_amount'")
		}
		p.increase_amount = amt.(int)
	}

	log.Printf("Part %s is initialized\n", p.Id())
	return nil
}

func (p *testPart) run() {
	log.Printf("Part %s starts running\n", p.Id())
loop:
	for {
		select {
		case msg := <-p.communicationChan:
			cmd := msg[0].(int)
			log.Printf("Received cmd=%d\n", cmd)
			respch := msg[1].(chan []interface{})
			switch cmd {
			case cmdStop:
				log.Println("Received Stop request");
				close(p.communicationChan)
				close(p.dataChan)
				respch <- []interface{}{true}
				break loop
			case cmdHeartBeat:
				respch <- []interface{}{true}
			}
		case data := <-p.dataChan:
			//async
			p.waitGrp.Add(1)
			go p.process(data)
		}
	}

}

func (p *testPart) process(data interface{}) {
	newData := data.(int) + p.increase_amount

	//raise DataProcessed event
	p.RaiseEvent(common.DataProcessed, data, p, nil, nil)

	p.Connector().Forward(newData)
	
	p.waitGrp.Done()
}

func (p *testPart) Stop() error {
	p.stateLock.Lock()
	defer p.stateLock.Unlock()

	err := p.stopSelf()
	if err != nil {
		return err
	}
	log.Printf("Part %s is stopped\n", p.Id())

	return err
}

func (p *testPart) stopSelf() error {
	respChan := make(chan []interface{})
	p.communicationChan <- []interface{}{cmdStop, respChan}
	response := <-respChan
	succeed := response[0].(bool)
	
	//wait for all the data processing go routine to finsh
	p.waitGrp.Wait()
	
	if succeed {
		p.isStarted = false
		log.Printf("Part %s is stopped\n", p.Id())
		return nil
	} else {
		error_msg := response[1].(string)
		log.Printf("Failed to stop part %s\n", p.Id())
		return errors.New(error_msg)
	}
}

func (p *testPart) Receive(data interface{}) error {
	log.Println("Data reached, try to send it to data channale")
	if p.dataChan == nil || !p.IsStarted() {
		return errors.New("The Part is not running, not ready to process data")
	}
	p.dataChan <- data
	log.Println("data Received")

	//raise DataReceived event
	p.RaiseEvent(common.DataReceived, data, p, nil, nil)

	return nil
}

//IsStarted returns true if the nozzle is started; otherwise returns false
func (p *testPart) IsStarted() bool {
	p.stateLock.Lock()
	defer p.stateLock.Unlock()

	return p.isStarted
}
