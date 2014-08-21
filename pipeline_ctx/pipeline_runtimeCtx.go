package pipeline_ctx

import (
	"sync"
	"errors"
)

type PipelineRuntimeCtx struct {
	//registered runtime pipeline service
	runtime_svcs map[string]common.PipelineService

	//pipeline
	pipeline common.Pipeline

	//the lock to serialize the request to start\stop the pipeline
	stateLock sync.Mutex

	isRunning bool
}

func New (p common.Pipeline) (*PipelineRuntimeCtx, error) {
	ctx := &PipelineRuntimeCtx {
		runtime_srcs : make (map[string]common.PipelineService),
		pipeline : p,
		isRunning : false
	}
	
	return ctx, nil
}

func (ctx *PipelineRuntimeCtx) Start(params map[string]interface{}) error {
	ctx.stateLock.Lock()
	defer ctx.stateLock.Unlock()

	var err error = nil
	//start all registered services
	for _, svc := range ctx.runtime_svcs {
		err = svc.Start(params)
		if err != nil {
			//TODO: log error
		}
	}

	if err == nil {
		ctx.isRunning = true
	} else {
		//clean up
		err2 := ctx.Stop()
		if err2 != nil {
			//failed to clean up
			panic("Pipeline runtime context failed to start up, try to clean up, failed again")
		}
	}
	return err
}

func (ctx *PipelineRuntimeCtx) Stop() error {
	ctx.stateLock.Lock()
	defer ctx.stateLock.Unlock()

	var err error = nil
	//stop all registered services
	for _, svc := range ctx.runtime_svcs {
		err = svc.Stop()
		if err != nil {
			//TODO: log error
		}
	}

	if err == nil {
		ctx.isRunning = false
	} else {
		panic("Pipeline runtime context failed to stop")
	}
	return err
}

func (ctx *PipelineRuntimeCtx) Pipeline() Pipeline {
	return ctx.pipeline
}

func (ctx *PipelineRuntimeCtx) Service(svc_name string) PipelineService {
	return ctx.runtime_svcs[svc_name]
}

func (ctx *PipelineRuntimeCtx) RegisterService(svc_name string, svc PipelineService) error {
	ctx.stateLock.Lock()
	defer ctx.stateLock.Unlock()

	if ctx.isRunning {
		return errors.New("Can't register service when PipelineRuntimeContext is already running")
	}

	return svc.Attach(ctx.pipeline)

}

func (ctx *PipelineRuntimeCtx) UnregisterService (srv_name string) error {
	ctx.stateLock.Lock()
	defer ctx.stateLock.Unlock()

	var err error
	svc := ctx.runtime_svcs[srv_name]
	
	if svc != nil && ctx.isRunning {
		err = svc.Stop()
		if (err != nil) {
			//log error
		}
	}
	
	//remove it from the map
	delete (ctx.runtime_svcs, srv_name)	
	
	return err	
}