package dht

import (
	"time"
)

// blockedItem represents a blocked node.
type blockedItem struct {
	ip         string
	port       int
	createTime time.Time
}

// blackList manages the blocked nodes including which sends bad information
// and can't ping out.
type blackList struct {
	list         *syncedMap
	maxSize      int			// 最多保存 maxSize 个黑名单条目
	expiredAfter time.Duration	// 过期时间
}

// newBlackList returns a blackList pointer.
func newBlackList(size int) *blackList {
	return &blackList{
		list:         newSyncedMap(),
		maxSize:      size,
		expiredAfter: time.Hour * 1,
	}
}

// genKey returns a key. If port is less than 0, the key wil be ip. Ohterwise
// it will be `ip:port` format.
func (bl *blackList) genKey(ip string, port int) string {
	key := ip
	if port >= 0 {
		key = genAddress(ip, port)
	}
	return key
}

// insert adds a blocked item to the blacklist.
func (bl *blackList) insert(ip string, port int) {
	// 容量限制，最多保存 maxSize 个黑名单条目
	if bl.list.Len() >= bl.maxSize {
		return
	}

	bl.list.Set(bl.genKey(ip, port), &blockedItem{
		ip:         ip,
		port:       port,
		createTime: time.Now(),
	})
}

// delete removes blocked item form the blackList.
func (bl *blackList) delete(ip string, port int) {
	bl.list.Delete(bl.genKey(ip, port))
}

// validate checks whether ip-port pair is in the block nodes list.
func (bl *blackList) in(ip string, port int) bool {
	// 查找 IP
	if _, ok := bl.list.Get(ip); ok {
		return true
	}

	// 查找 IP/Port
	key := bl.genKey(ip, port)
	v, ok := bl.list.Get(key)
	if ok {
		// 是否过期
		if time.Now().Sub(v.(*blockedItem).createTime) < bl.expiredAfter {
			return true
		}
		// 移除过期条目
		bl.list.Delete(key)
	}

	return false
}

// clear cleans the expired items every 10 minutes.
//
// 每隔 10 min 移除黑名单中的过期条目
func (bl *blackList) clear() {
	for _ = range time.Tick(time.Minute * 10) {
		keys := make([]interface{}, 0, 100)
		for item := range bl.list.Iter() {
			if time.Now().Sub(
				item.val.(*blockedItem).createTime) > bl.expiredAfter {
				keys = append(keys, item.key)
			}
		}
		bl.list.DeleteMulti(keys)
	}
}
