package dht

import (
	"container/heap"
	"errors"
	"net"
	"strings"
	"sync"
	"time"
)

// maxPrefixLength is the length of DHT node.
const maxPrefixLength = 160

// node represents a DHT node.
type node struct {
	id             *bitmap
	addr           *net.UDPAddr
	lastActiveTime time.Time
}

// newNode returns a node pointer.
func newNode(id, network, address string) (*node, error) {
	if len(id) != 20 {
		return nil, errors.New("node id should be a 20-length string")
	}

	// 通过 net.ResolveUDPAddr 创建监听地址
	addr, err := net.ResolveUDPAddr(network, address)
	if err != nil {
		return nil, err
	}

	//
	return &node{newBitmapFromString(id), addr, time.Now()}, nil
}

// newNodeFromCompactInfo parses compactNodeInfo and returns a node pointer.
func newNodeFromCompactInfo(
	compactNodeInfo string, network string) (*node, error) {

	if len(compactNodeInfo) != 26 {
		return nil, errors.New("compactNodeInfo should be a 26-length string")
	}

	id := compactNodeInfo[:20]
	ip, port, _ := decodeCompactIPPortInfo(compactNodeInfo[20:])

	return newNode(id, network, genAddress(ip.String(), port))
}

// CompactIPPortInfo returns "Compact IP-address/port info".
// See http://www.bittorrent.org/beps/bep_0005.html.
func (node *node) CompactIPPortInfo() string {
	// <IP:Port>
	info, _ := encodeCompactIPPortInfo(node.addr.IP, node.addr.Port)
	return info
}

// CompactNodeInfo returns "Compact node info".
// See http://www.bittorrent.org/beps/bep_0005.html.
func (node *node) CompactNodeInfo() string {
	// <NodeID><IP:Port>
	return strings.Join([]string{node.id.RawString(), node.CompactIPPortInfo()}, "")
}

// Peer represents a peer contact.
type Peer struct {
	IP    net.IP
	Port  int
	token string
}

// newPeer returns a new peer pointer.
func newPeer(ip net.IP, port int, token string) *Peer {
	return &Peer{
		IP:    ip,
		Port:  port,
		token: token,
	}
}

// newPeerFromCompactIPPortInfo create a peer pointer by compact ip/port info.
func newPeerFromCompactIPPortInfo(compactInfo, token string) (*Peer, error) {
	// 解析 IP:Port
	ip, port, err := decodeCompactIPPortInfo(compactInfo)
	if err != nil {
		return nil, err
	}
	// 构造 Peer
	return newPeer(ip, port, token), nil
}

// CompactIPPortInfo returns "Compact node info".
// See http://www.bittorrent.org/beps/bep_0005.html.
func (p *Peer) CompactIPPortInfo() string {
	info, _ := encodeCompactIPPortInfo(p.IP, p.Port)
	return info
}

// peersManager represents a proxy that manipulates peers.
type peersManager struct {
	sync.RWMutex
	table *syncedMap
	dht   *DHT
}

// newPeersManager returns a new peersManager.
func newPeersManager(dht *DHT) *peersManager {
	return &peersManager{
		table: newSyncedMap(),
		dht:   dht,
	}
}

// Insert adds a peer into peersManager.
func (pm *peersManager) Insert(infoHash string, peer *Peer) {
	pm.Lock()
	if _, ok := pm.table.Get(infoHash); !ok {
		pm.table.Set(infoHash, newKeyedDeque())
	}
	pm.Unlock()

	v, _ := pm.table.Get(infoHash)
	queue := v.(*keyedDeque)

	queue.Push(peer.CompactIPPortInfo(), peer)
	if queue.Len() > pm.dht.K {
		queue.Remove(queue.Front())
	}
}

// GetPeers returns size-length peers who announces having infoHash.
func (pm *peersManager) GetPeers(infoHash string, size int) []*Peer {
	peers := make([]*Peer, 0, size)

	// 根据 infoHash 查找缓存
	v, ok := pm.table.Get(infoHash)
	if !ok {
		return peers
	}

	// 转换 peers 列表
	for e := range v.(*keyedDeque).Iter() {
		peers = append(peers, e.Value.(*Peer))
	}

	// 截断
	if len(peers) > size {
		peers = peers[len(peers)-size:]
	}

	return peers
}

// kbucket represents a k-size bucket.
//
// k-桶
type kbucket struct {
	sync.RWMutex
	nodes       *keyedDeque	// 节点
	candidates  *keyedDeque // 候选节点
	lastChanged time.Time   // 最后更新时间
	prefix      *bitmap     // 前缀
}

// newKBucket returns a new kbucket pointer.
func newKBucket(prefix *bitmap) *kbucket {
	bucket := &kbucket{
		nodes:       newKeyedDeque(),
		candidates:  newKeyedDeque(),
		lastChanged: time.Now(),
		prefix:      prefix,
	}
	return bucket
}

// LastChanged return the last time when it changes.
func (bucket *kbucket) LastChanged() time.Time {
	bucket.RLock()
	defer bucket.RUnlock()
	return bucket.lastChanged
}

// RandomChildID returns a random id that has the same prefix with bucket.
func (bucket *kbucket) RandomChildID() string {
	prefixLen := bucket.prefix.Size / 8
	return strings.Join([]string{
		bucket.prefix.RawString()[:prefixLen],
		randomString(20 - prefixLen),
	}, "")
}

// UpdateTimestamp update bucket's last changed time..
func (bucket *kbucket) UpdateTimestamp() {
	bucket.Lock()
	defer bucket.Unlock()

	bucket.lastChanged = time.Now()
}

// Insert inserts node to the bucket. It returns whether the node is new in
// the bucket.
func (bucket *kbucket) Insert(no *node) bool {
	isNew := !bucket.nodes.HasKey(no.id.RawString())

	bucket.nodes.Push(no.id.RawString(), no)
	bucket.UpdateTimestamp()

	return isNew
}

// Replace removes node, then put bucket.candidates.Back() to the right
// place of bucket.nodes.
func (bucket *kbucket) Replace(no *node) {
	bucket.nodes.Delete(no.id.RawString())
	bucket.UpdateTimestamp()

	if bucket.candidates.Len() == 0 {
		return
	}

	no = bucket.candidates.Remove(bucket.candidates.Back()).(*node)

	inserted := false
	for e := range bucket.nodes.Iter() {
		if e.Value.(*node).lastActiveTime.After(
			no.lastActiveTime) && !inserted {

			bucket.nodes.InsertBefore(no, e)
			inserted = true
		}
	}

	if !inserted {
		bucket.nodes.PushBack(no)
	}
}

// Fresh pings the expired nodes in the bucket.
func (bucket *kbucket) Fresh(dht *DHT) {
	for e := range bucket.nodes.Iter() {
		no := e.Value.(*node)
		if time.Since(no.lastActiveTime) > dht.NodeExpriedAfter {
			dht.transactionManager.ping(no)
		}
	}
}

// routingTableNode represents routing table tree node.
type routingTableNode struct {
	sync.RWMutex
	children []*routingTableNode	// 子节点列表，因为是二叉树，最多有 2 个子节点
	bucket   *kbucket				// k-桶
}

// newRoutingTableNode returns a new routingTableNode pointer.
func newRoutingTableNode(prefix *bitmap) *routingTableNode {
	return &routingTableNode{
		children: make([]*routingTableNode, 2),
		bucket:   newKBucket(prefix),
	}
}

// Child returns routingTableNode's left or right child.
func (tableNode *routingTableNode) Child(index int) *routingTableNode {
	// 参数检查，二叉树只有 2 个子节点
	if index >= 2 {
		return nil
	}
	// 获取第 index 子节点
	tableNode.RLock()
	defer tableNode.RUnlock()
	return tableNode.children[index]
}

// SetChild sets routingTableNode's left or right child. When index is 0, it's
// the left child, if 1, it's the right child.
func (tableNode *routingTableNode) SetChild(index int, c *routingTableNode) {
	// 设置第 index 子节点
	tableNode.Lock()
	defer tableNode.Unlock()
	tableNode.children[index] = c
}

// KBucket returns the bucket routingTableNode holds.
func (tableNode *routingTableNode) KBucket() *kbucket {
	tableNode.RLock()
	defer tableNode.RUnlock()
	return tableNode.bucket
}

// SetKBucket sets the bucket.
func (tableNode *routingTableNode) SetKBucket(bucket *kbucket) {
	tableNode.Lock()
	defer tableNode.Unlock()
	tableNode.bucket = bucket
}

// Split splits current routingTableNode and sets it's two children.
func (tableNode *routingTableNode) Split() {

	// 位于 kad 数的第几层
	prefixLen := tableNode.KBucket().prefix.Size

	// 如果是最底层，就不要再分裂了
	if prefixLen == maxPrefixLength {
		return
	}

	// 分裂为左右两个子节点
	for i := 0; i < 2; i++ {
		// 创建一个有 prefixLen+1 个 bits 的位图，并将前 prefixLen 个 bit 拷贝过来。
		bmap := newBitmapFrom(tableNode.KBucket().prefix, prefixLen+1)
		//
		rtnode := newRoutingTableNode(bmap)
		//
		tableNode.SetChild(i, rtnode)
	}

	tableNode.Lock()
	tableNode.children[1].bucket.prefix.Set(prefixLen)
	tableNode.Unlock()

	for e := range tableNode.KBucket().nodes.Iter() {
		nd := e.Value.(*node)
		tableNode.Child(nd.id.Bit(prefixLen)).KBucket().nodes.PushBack(nd)
	}

	for e := range tableNode.KBucket().candidates.Iter() {
		nd := e.Value.(*node)
		tableNode.Child(nd.id.Bit(prefixLen)).KBucket().candidates.PushBack(nd)
	}

	for i := 0; i < 2; i++ {
		tableNode.Child(i).KBucket().UpdateTimestamp()
	}
}

// routingTable implements the routing table in DHT protocol.
//
// 路由表
type routingTable struct {
	*sync.RWMutex
	k              int
	root           *routingTableNode
	cachedNodes    *syncedMap
	cachedKBuckets *keyedDeque
	dht            *DHT
	clearQueue     *syncedList
}

// newRoutingTable returns a new routingTable pointer.
func newRoutingTable(k int, dht *DHT) *routingTable {
	root := newRoutingTableNode(newBitmap(0))

	rt := &routingTable{
		RWMutex:        &sync.RWMutex{},
		k:              k,
		root:           root,
		cachedNodes:    newSyncedMap(),
		cachedKBuckets: newKeyedDeque(),
		dht:            dht,
		clearQueue:     newSyncedList(),
	}

	rt.cachedKBuckets.Push(root.bucket.prefix.String(), root.bucket)
	return rt
}

// Insert adds a node to routing table. It returns whether the node is new
// in the routingtable.
func (rt *routingTable) Insert(nd *node) bool {
	rt.Lock()
	defer rt.Unlock()

	// 如果是黑名单节点、或者已缓存节点数达到上限，就拒绝插入
	if rt.dht.blackList.in(nd.addr.IP.String(), nd.addr.Port) ||
		rt.cachedNodes.Len() >= rt.dht.MaxNodes {
		return false
	}

	var (
		next   *routingTableNode
		bucket *kbucket
	)

	// 从顶往下，深搜二叉树。
	//
	// 整个二叉树的深度最大为 160 ，所以最多循环 160 次，每次循环向下定位到一棵子树。
	//
	root := rt.root
	for prefixLen := 1; prefixLen <= maxPrefixLength; prefixLen++ {
		// [定位子树] 取 NodeId 第 i 个 bit 值( 0 or 1 ) ，定位到是 root 的左子树，还是右子树。
		next = root.Child(nd.id.Bit(prefixLen - 1))

		// 子树非空，继续向下搜索。
		if next != nil {
			// If next is not the leaf.
			root = next
		// 子树为空，但是当前树的k-桶未满或者 NodeId 已经存在于 k-桶中，则直接插入。
		} else if root.KBucket().nodes.Len() < rt.k || root.KBucket().nodes.HasKey(nd.id.RawString()) {

			bucket = root.KBucket()
			isNew := bucket.Insert(nd)

			rt.cachedNodes.Set(nd.addr.String(), nd)
			rt.cachedKBuckets.Push(bucket.prefix.String(), bucket)
			return isNew

		// 子树为空，但是当前树的k-桶已满且 NodeId 不存在于 k-桶中，且
		} else if root.KBucket().prefix.Compare(nd.id, prefixLen-1) == 0 {
			// If node has the same prefix with bucket, split it.

			root.Split()

			rt.cachedKBuckets.Delete(root.KBucket().prefix.String())
			root.SetKBucket(nil)

			for i := 0; i < 2; i++ {
				bucket = root.Child(i).KBucket()
				rt.cachedKBuckets.Push(bucket.prefix.String(), bucket)
			}

			root = root.Child(nd.id.Bit(prefixLen - 1))
		} else {


			// Finally, store node as a candidate and fresh the bucket.
			root.KBucket().candidates.PushBack(nd)
			if root.KBucket().candidates.Len() > rt.k {
				root.KBucket().candidates.Remove(
					root.KBucket().candidates.Front())
			}

			go root.KBucket().Fresh(rt.dht)
			return false
		}

	}
	return false
}

// GetNeighbors returns the size-length nodes closest to id.
func (rt *routingTable) GetNeighbors(id *bitmap, size int) []*node {
	//
	rt.RLock()
	nodes := make([]interface{}, 0, rt.cachedNodes.Len())
	for item := range rt.cachedNodes.Iter() {
		nodes = append(nodes, item.val.(*node))
	}
	rt.RUnlock()

	// 从 nodes 中查找距离 id 最近的 size 个节点。
	neighbors := getTopK(nodes, id, size)

	// 类型转换: []interface{} => []*node
	result := make([]*node, len(neighbors))
	for i, nd := range neighbors {
		result[i] = nd.(*node)
	}

	// 返回
	return result
}

// GetNeighborCompactInfos return the size-length compact node info closest to id.
func (rt *routingTable) GetNeighborCompactInfos(id *bitmap, size int) []string {

	// 从 nodes 中查找距离 id 最近的 size 个节点。
	neighbors := rt.GetNeighbors(id, size)

	// 格式转换: nodes => list[<NodeID><IP:Port>]
	infos := make([]string, len(neighbors))
	for i, no := range neighbors {
		// info = <NodeID><IP:Port>
		infos[i] = no.CompactNodeInfo()
	}

	return infos
}

// GetNodeKBucketByID returns node whose id is `id` and the bucket it
// belongs to.
func (rt *routingTable) GetNodeKBucketByID(id *bitmap) (nd *node, bucket *kbucket) {

	rt.RLock()
	defer rt.RUnlock()

	var next *routingTableNode
	root := rt.root

	// 最多搜寻 160 个 bit
	for prefixLen := 1; prefixLen <= maxPrefixLength; prefixLen++ {

		// 取第 i 个 bit 值( 0 or 1 ) ，进而定位到是 root 的左子树，还是右子树。
		next = root.Child(id.Bit(prefixLen - 1))

		// 为空，则查找结束
		if next == nil {

			//
			v, ok := root.KBucket().nodes.Get(id.RawString())
			if !ok {
				return
			}

			nd, bucket = v.Value.(*node), root.KBucket()
			return
		}
		root = next
	}
	return
}

// GetNodeByAddress finds node by address.
func (rt *routingTable) GetNodeByAddress(address string) (no *node, ok bool) {
	rt.RLock()
	defer rt.RUnlock()

	v, ok := rt.cachedNodes.Get(address)
	if ok {
		no = v.(*node)
	}
	return
}

// Remove deletes the node whose id is `id`.
func (rt *routingTable) Remove(id *bitmap) {
	if nd, bucket := rt.GetNodeKBucketByID(id); nd != nil {
		bucket.Replace(nd)
		rt.cachedNodes.Delete(nd.addr.String())
		rt.cachedKBuckets.Push(bucket.prefix.String(), bucket)
	}
}

// RemoveByAddr deletes the node whose address is `ip:port`.
func (rt *routingTable) RemoveByAddr(address string) {
	v, ok := rt.cachedNodes.Get(address)
	if ok {
		rt.Remove(v.(*node).id)
	}
}

// Fresh sends findNode to all nodes in the expired nodes.
func (rt *routingTable) Fresh() {
	now := time.Now()

	for e := range rt.cachedKBuckets.Iter() {
		bucket := e.Value.(*kbucket)
		if now.Sub(bucket.LastChanged()) < rt.dht.KBucketExpiredAfter ||
			bucket.nodes.Len() == 0 {
			continue
		}

		i := 0
		for e := range bucket.nodes.Iter() {
			if i < rt.dht.RefreshNodeNum {
				no := e.Value.(*node)
				rt.dht.transactionManager.findNode(no, bucket.RandomChildID())
				rt.clearQueue.PushBack(no)
			}
			i++
		}
	}

	if rt.dht.IsCrawlMode() {
		for e := range rt.clearQueue.Iter() {
			rt.Remove(e.Value.(*node).id)
		}
	}

	rt.clearQueue.Clear()
}

// Len returns the number of nodes in table.
func (rt *routingTable) Len() int {
	rt.RLock()
	defer rt.RUnlock()

	return rt.cachedNodes.Len()
}

// Implementation of heap with heap.Interface.
type heapItem struct {
	distance *bitmap		// XOR 距离
	value    interface{}
}

type topKHeap []*heapItem

func (kHeap topKHeap) Len() int {
	return len(kHeap)
}

func (kHeap topKHeap) Less(i, j int) bool {
	return kHeap[i].distance.Compare(kHeap[j].distance, maxPrefixLength) == 1
}

func (kHeap topKHeap) Swap(i, j int) {
	kHeap[i], kHeap[j] = kHeap[j], kHeap[i]
}

func (kHeap *topKHeap) Push(x interface{}) {
	*kHeap = append(*kHeap, x.(*heapItem))
}

func (kHeap *topKHeap) Pop() interface{} {
	n := len(*kHeap)
	x := (*kHeap)[n-1]
	*kHeap = (*kHeap)[:n-1]
	return x
}

// getTopK solves the top-k problem with heap. It's time complexity is
// O(n*log(k)). When n is large, time complexity will be too high, need to be
// optimized.
//
// 从 queue 中查找距离 id 最近的 k 个元素。
//
func getTopK(queue []interface{}, id *bitmap, k int) []interface{} {

	topkHeap := make(topKHeap, 0, k+1)

	// 遍历 queue ，构造 topKHeap
	for _, value := range queue {
		node := value.(*node)

		// 计算 id 和 node.Id 的 XOR 距离
		distance := id.Xor(node.id)

		// 如果 Heap 已满，酌情替换
		if topkHeap.Len() == k {
			// 取尾部元素 last
			var last = topkHeap[topkHeap.Len()-1]
			// 如果 last 比 node 的距离更远，就替换它
			if last.distance.Compare(distance, maxPrefixLength) == 1 {
				item := &heapItem{
					distance,
					value,
				}
				heap.Push(&topkHeap, item)
				heap.Pop(&topkHeap)
			}
			// 如果 Heap 未满，直接添加
		} else {
			item := &heapItem{
				distance,
				value,
			}
			heap.Push(&topkHeap, item)
		}
	}

	// 遍历 topKHeap ，按序输出 top k list
	tops := make([]interface{}, topkHeap.Len())
	for i := len(tops) - 1; i >= 0; i-- {
		tops[i] = heap.Pop(&topkHeap).(*heapItem).value
	}

	return tops
}
