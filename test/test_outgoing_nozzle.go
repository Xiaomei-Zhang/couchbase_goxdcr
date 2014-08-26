package test

import (
	"errors"
	"fmt"
	common "github.com/Xiaomei-Zhang/couchbase_goxdcr/common"
	part "github.com/Xiaomei-Zhang/couchbase_goxdcr/part"
	"log"
	"sync"
)

type testOutgoingNozzle struct {
	part.AbstractPart

	dataChan chan interface{}
	//communication channel
	communicationChan chan []interface{}

	isOpen    bool
	isStarted bool

	//the lock to serialize the request to open\close the nozzle
	stateLock sync.Mutex
	
	waitGrp sync.WaitGroup
}

func newOutgoingNozzle(id string) *testOutgoingNozzle {
	return &testOutgoingNozzle{part.NewAbstractPart(id), nil, nil, false, false, sync.Mutex{}, sync.WaitGroup{}}
}

func (p *testOutgoingNozzle) Start(settings map[string]interface{}) error {
	log.Printf("Try to start part %s\n", p.Id())
	p.stateLock.Lock()
	defer p.stateLock.Unlock()

	p.init(settings)
	go p.run()
	p.isStarted = true
	return nil
}

func (p *testOutgoingNozzle) init(settings map[string]interface{}) error {
	p.dataChan = make (chan interface{})
	p.communicationChan = make(chan []interface{})
	return nil
}

func (p *testOutgoingNozzle) run() {
	log.Printf("Part %s is running\n", p.Id())
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
				respch <- []interface{}{true}
				break loop
			case cmdHeartBeat:
				respch <- []interface{}{true}
			}
		case data := <-p.dataChan:
			if p.IsOpen() {
				//async
				p.waitGrp.Add(1)
				go p.printData(data)
			}
		}
	}

}

func (p *testOutgoingNozzle) printData(data interface{}) {
	log.Printf("Send out data %d\n", data.(int))
	fmt.Printf("data: %d\n", data.(int))
	p.RaiseEvent(common.DataSent, data, p, nil, nil)
	
	p.waitGrp.Done()
}

func (p *testOutgoingNozzle) Stop() error {
	p.stateLock.Lock()
	defer p.stateLock.Unlock()

	err := p.stopSelf()
	if err != nil {
		return err
	}
	log.Printf("Part %s is stopped\n", p.Id())
	
	return err
}

func (p *testOutgoingNozzle) stopSelf() error {
	respChan := make(chan []interface{})
	p.communicationChan <- []interface{}{cmdStop, respChan}
	response := <-respChan
	succeed := response[0].(bool)
	
	//wait for all spawned data-process goroutines to finish
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


//Data can be passed to the downstream
func (p *testOutgoingNozzle) Open() error {
	p.stateLock.Lock()
	defer p.stateLock.Unlock()

	p.isOpen = true
	return nil
}

//Close closes the Nozzle
//
//Data can get to this nozzle, but would not be passed to the downstream
func (p *testOutgoingNozzle) Close() error {
	p.stateLock.Lock()
	defer p.stateLock.Unlock()

	p.isOpen = false
	return nil
}

//IsOpen returns true if the nozzle is open; returns false if the nozzle is closed
func (p *testOutgoingNozzle) IsOpen() bool {
	p.stateLock.Lock()
	defer p.stateLock.Unlock()

	return p.isOpen
}

//IsStarted returns true if the nozzle is started; otherwise returns false
func (p *testOutgoingNozzle) IsStarted() bool {
	p.stateLock.Lock()
	defer p.stateLock.Unlock()

	return p.isStarted
}

func (p *testOutgoingNozzle) Receive(data interface{}) error {
	log.Println("Data reached, try to send it to data channale")
	if p.dataChan == nil {
		return errors.New("The Part is not running, not ready to process data")
	}
	p.dataChan <- data
	log.Println("data Received")

	//raise DataReceived event
	p.RaiseEvent(common.DataReceived, data, p, nil, nil)

	return nil
}
