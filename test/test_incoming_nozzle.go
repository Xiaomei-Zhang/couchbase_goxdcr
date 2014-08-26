package test

import (
	"errors"
	"fmt"
	common "github.com/Xiaomei-Zhang/couchbase_goxdcr/common"
	part "github.com/Xiaomei-Zhang/couchbase_goxdcr/part"
	"log"
	"reflect"
	"sync"
)

type testIncomingNozzle struct {
	part.AbstractPart
	//settings
	start_int int

	//communication channel
	communicationChan chan []interface{}

	isOpen    bool
	isStarted bool

	//the lock to serialize the request to open\close the nozzle
	stateLock sync.Mutex

	waitGrp sync.WaitGroup
}

func newInComingNozzle(id string) *testIncomingNozzle {
	return &testIncomingNozzle{part.NewAbstractPart(id), 0, nil, false, false, sync.Mutex{}, sync.WaitGroup{}}
}

func (p *testIncomingNozzle) Start(settings map[string]interface{}) error {
	p.stateLock.Lock()
	defer p.stateLock.Unlock()

	p.init(settings)
	go p.run(p.communicationChan)
	p.isStarted = true
	return nil
}

func (p *testIncomingNozzle) init(settings map[string]interface{}) error {
	p.communicationChan = make(chan []interface{}, 2)

	start, ok := settings["start_int"]
	if ok {
		if reflect.TypeOf(start).Name() != "int" {
			return errors.New("Wrong parameter. Expects int as the value for 'start_int'")
		}
		p.start_int = start.(int)
	}
	return nil
}

func (p *testIncomingNozzle) run(msgChan chan []interface{}) {
	log.Println("run with msgChan = " + fmt.Sprint(msgChan))
	counter := 0
loop:
	for {
//		log.Println("Loop " + fmt.Sprint(counter))
		select {
		case msg := <-msgChan:
			cmd := msg[0].(int)
			log.Printf("Received cmd=%d\n", cmd)
			respch := msg[1].(chan []interface{})
			switch cmd {
			case cmdStop:
				log.Println("Received Stop request")
				close(msgChan)
				respch <- []interface{}{true}
				break loop
			case cmdHeartBeat:
				respch <- []interface{}{true}
			}
		default:
			if p.IsOpen() && counter == 100 {
				//async
				p.waitGrp.Add(1)
				go p.pumpData()
				counter = 0
			}
		}
		counter++
	}
}

func (p *testIncomingNozzle) pumpData() {
	//raise DataReceived event
	if p.start_int < 1000 {
		p.start_int++
		//raise DataReceived event
		p.RaiseEvent(common.DataReceived, p.start_int, p, nil, nil)

		log.Printf("Pump data: %d\n", p.start_int)
		p.Connector().Forward(p.start_int)
	}

	p.waitGrp.Done()
}

func (p *testIncomingNozzle) Stop() error {
	p.stateLock.Lock()
	defer p.stateLock.Unlock()

	log.Printf("Try to stop part %s", p.Id())
	err := p.stopSelf()
	if err != nil {
		return err
	}
	log.Printf("Part %s is stopped\n", p.Id())

	return err
}

func (p *testIncomingNozzle) stopSelf() error {
	log.Println("Start stopSelf.... msgChan = " + fmt.Sprint(p.communicationChan))
	respChan := make(chan []interface{})
	p.communicationChan <- []interface{}{cmdStop, respChan}
	log.Println("send Stop request to communication channel " + fmt.Sprint(p.communicationChan))
	response := <-respChan
	succeed := response[0].(bool)

	//wait all the data-pumping routines have finished
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
func (p *testIncomingNozzle) Open() error {
	p.stateLock.Lock()
	defer p.stateLock.Unlock()

	p.isOpen = true
	return nil
}

//Close closes the Nozzle
//
//Data can get to this nozzle, but would not be passed to the downstream
func (p *testIncomingNozzle) Close() error {
	p.stateLock.Lock()
	defer p.stateLock.Unlock()

	p.isOpen = false

	log.Printf("Nozzle %s is closed\n", p.Id())
	return nil
}

//IsOpen returns true if the nozzle is open; returns false if the nozzle is closed
func (p *testIncomingNozzle) IsOpen() bool {
	p.stateLock.Lock()
	defer p.stateLock.Unlock()

	return p.isOpen
}

//IsStarted returns true if the nozzle is started; otherwise returns false
func (p *testIncomingNozzle) IsStarted() bool {
	p.stateLock.Lock()
	defer p.stateLock.Unlock()

	return p.isStarted
}

func (p *testIncomingNozzle) Receive(data interface{}) error {
	return errors.New("Incoming nozzle doesn't have upstream")
}
