package feed_manager

import (
	common "github.com/Xiaomei-Zhang/couchbase_goxdcr/common"
	"sync"
)

type feedManager struct {
	feed_factory common.FeedFactory
	live_feeds   map[string]common.Feed

	once sync.Once
}

var feed_mgr feedManager

func FeedManager(factory common.FeedFactory) {
	feed_mgr.once.Do(func() {
		feed_mgr.feed_factory = factory
	})
}

func StartFeed(topic string) error {
	return feed_mgr.startFeed(topic)
}

func StopFeed(topic string) error {
	return feed_mgr.stopFeed(topic)
}

func RuntimeCtx(topic string) common.FeedRuntimeContext {
	return feed_mgr.runtimeCtx(topic)
}

func (feddMgr *feedManager) startFeed(topic string) error {
	var err error
	if f, ok := feedMgr.live_feeds[topic]; !ok {
		f = feedMgr.feed_factory.NewFeed(topic)
		err = f.Start()
		if err != nil {
			return err
		}
		feedMgr.live_feeds[topic] = f
	}else {
		//the feed is already running
		//TODO: log
	}
	return err
}

func (feedMgr *feedManager) stopFeed(topic string) error {
	var err error
	if f, ok := feedMgr.live_feeds[topic]; ok{
		f.Stop ()
		delete (feedMgr.live_feeds, topic)
	}else {
		//The named feed is not active
		//TODO: log
	}
	return err
}

func (feedMgr *feedManager) runtimeCtx(topic string) common.FeedRuntimeContext {
	feed := feedMgr.live_feeds[topic]
	if feed != nil {
		return feed.RuntimeContext()
	}

	return nil
}

//func (feedMgr *feedManager) delFeed(topic string) error {
//	delete(feedMgr.live_feeds, topic)
//	return nil
//}
