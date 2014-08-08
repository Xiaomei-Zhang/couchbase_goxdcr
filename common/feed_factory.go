package common

import (

)

type FeedFactory interface {
	NewFeed (topic string) Feed
}

