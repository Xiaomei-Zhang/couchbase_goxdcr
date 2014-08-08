package common

import (

)
//Any processing step which has a downstream step would need to implement this
//for example secondary index's KVFeed would need to implement this
type Connectable interface {
	Connector() Connector
	SetConnector(connector Connector)
}
