package dht

import (
	"errors"
	"net"
	"strings"
	"sync"
	"time"
)



// BEP-0005 协议
//
// 一条 KRPC 消息可以代表请求 request ，也可以代表响应 response ，由字典组成。
//
//
//
// ping:
//	检测节点是否可达，请求包含一个参数 id ，代表该节点的 nodeID ，对应的回复也应该包含回复者的 nodeID 。
//
//	ping Query = {"t":"aa", "y":"q", "q":"ping", "a":{"id":"abcdefghij0123456789"}}
//	bencoded = d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe
//
//	ping Response = {"t":"aa", "y":"r", "r": {"id":"mnopqrstuvwxyz123456"}}
//	bencoded = d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re
//
//
// find_node:
//	find_node 被用来查找给定 ID 的 DHT 节点的联系信息，该请求包含两个参数 id (代表该节点的 nodeID )和 target 。
//	回复中应该包含被请求节点的路由表中距离 target 最接近的 K 个 nodeID 以及对应的 nodeINFO 。
//
// 	find_node 请求包含 2 个参数，第一个参数是 id，包含了请求节点的ID；第二个参数是 target，包含了请求者正在查找的节点的 ID 。
//
//	当一个节点接收到了 find_node 的请求，他应该给出对应的回复，回复中包含 2 个关键字 id(被请求节点的id) 和 nodes，
//  其中	nodes 是字符串类型，包含了被请求节点的路由表中最接近目标节点的 K(8) 个最接近的节点的联系信息，
//  被请求方每次都统一返回最靠近目标节点的节点列表 K 桶。
//
//	参数: {"id" : "<querying nodes id>", "target" : "<id of target node>"}
//	回复: {"id" : "<queried nodes id>", "nodes" : "<compact node info>"}
//
//
//	这里要明确3个概念
//		1. 请求方的 id : 发起这个 DHT 节点寻址的节点自身的 ID ，可以类比 DNS 查询中的客户端
//		2. 目标 target id : 需要查询的目标ID号，可以类比于 DNS 查询中的 URL ，这个 ID 在整个递归查询中是一直不变的
//		3. 被请求节点的 id : 在节点的递归查询中，请求方由远及近不断询问整个链路上的节点，沿途的每个节点在返回时都要带上自己的 id 号
//
//	find_node Query = {"t":"aa", "y":"q", "q":"find_node", "a": {"id":"abcdefghij0123456789", "target":"mnopqrstuvwxyz123456"}}
//	# "id" containing the node ID of the querying node, and "target" containing the ID of the node sought by the queryer.
//	bencoded = d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t2:aa1:y1:qe
//
//	find_node Response = {"t":"aa", "y":"r", "r": {"id":"0123456789abcdefghij", "nodes": "def456..."}}
//	bencoded = d1:rd2:id20:0123456789abcdefghij5:nodes9:def456...e1:t2:aa1:y1:re
//
//
//
// get_peers
//
//	1. get_peers 请求包含 2 个参数：
//		- id 请求节点 ID
//		- info_hash 代表 torrent 文件的 infohash ，infohash 为种子文件的 SHA1 哈希值，也就是磁力链接的 btih 值
//	2. get_peers 响应:
//    1) 如果被请求的节点有对应 info_hash 的 peers，他将返回一个关键字 values，这是一个列表类型的字符串。
//       每一个字符串包含了 "CompactIP-address/portinfo" 格式的 peers 信息(即对应的机器ip/port信息)，
//       peer 的 info 信息和 DHT 节点的 info 信息是一样的。
//    2) 如果被请求的节点没有这个 infohash 的 peers ，那么他将返回关键字 nodes 。
//       需要注意的是，如果该节点没有对应的 infohash 信息，而只是返回了nodes，
//       则请求方会认为该节点是一个"可疑节点"，则会从自己的路由表 K 捅中删除该节点，
//       这个 nodes 包含了被请求节点的路由表中离 info_hash 最近的 K 个节点(我这里没有该节点，去别的节点试试运气)，
//       nodes 使用 "Compactnodeinfo" 格式编码，在这两种情况下，关键字 token 都将被返回。
//       token 关键字在今后的 annouce_peer 请求中必须要携带。
//       token 是一个短的二进制字符串。
//
//
//
const (
	// 检测节点是否可达
	pingType         = "ping"

	// 根据 ID 查找节点。
	// 被请求节点会回复其路由表中，距离给定 ID 最近的 K 个 nodeId 信息，并且被请求节点会将请求节点的信息加入到自身的路由表中。
	findNodeType     = "find_node"

	// 请求获取 info_hash 信息
	getPeersType     = "get_peers"

	// 表明正在下载 torrent 文件
	announcePeerType = "announce_peer"
)

const (
	generalError = 201 + iota
	serverError
	protocolError
	unknownError
)

// packet represents the information receive from udp.
//
// 代表从 Udp Socket 上接收的 Packet
type packet struct {
	data  []byte
	raddr *net.UDPAddr
}

// token represents the token when response getPeers request.
type token struct {
	data       string
	createTime time.Time
}

// tokenManager managers the tokens.
type tokenManager struct {
	*syncedMap
	expiredAfter time.Duration
	dht          *DHT
}

// newTokenManager returns a new tokenManager.
func newTokenManager(expiredAfter time.Duration, dht *DHT) *tokenManager {
	return &tokenManager{
		syncedMap:    newSyncedMap(),
		expiredAfter: expiredAfter,
		dht:          dht,
	}
}

// token returns a token. If it doesn't exist or is expired, it will add a
// new token.
func (tm *tokenManager) token(addr *net.UDPAddr) string {
	// 根据 IP 地址查询 Token
	v, ok := tm.Get(addr.IP.String())
	tk, _ := v.(token)

	// 如果 Token 不存在或者已过期，就新建 Token 并更新
	if !ok || time.Now().Sub(tk.createTime) > tm.expiredAfter {
		tk = token{
			data:       randomString(5),
			createTime: time.Now(),
		}
		tm.Set(addr.IP.String(), tk)
	}

	// 返回 Token
	return tk.data
}

// clear removes expired tokens.
//
// 每隔 3 min 移除 Token 表中的过期条目
func (tm *tokenManager) clear() {
	for _ = range time.Tick(time.Minute * 3) {
		keys := make([]interface{}, 0, 100)
		for item := range tm.Iter() {
			if time.Now().Sub(item.val.(token).createTime) > tm.expiredAfter {
				keys = append(keys, item.key)
			}
		}
		tm.DeleteMulti(keys)
	}
}

// check returns whether the token is valid.
func (tm *tokenManager) check(addr *net.UDPAddr, tokenString string) bool {
	key := addr.IP.String()
	v, ok := tm.Get(key)
	tk, _ := v.(token)

	if ok {
		tm.Delete(key)
	}

	return ok && tokenString == tk.data
}

// makeQuery returns a query-formed data.
//
// 构造查询请求
func makeQuery(t, q string, a map[string]interface{}) map[string]interface{} {
	return map[string]interface{}{
		"t": t,		// transId
		"y": "q",	// query
		"q": q,		// query type
		"a": a,		// query params
	}
}

// makeResponse returns a response-formed data.
//
// 构造响应
func makeResponse(t string, r map[string]interface{}) map[string]interface{} {
	return map[string]interface{}{
		"t": t,		// transactionID
		"y": "r",	// response type
		"r": r,		// response params
	}
}

// makeError returns a err-formed data.
func makeError(t string, errCode int, errMsg string) map[string]interface{} {
	return map[string]interface{}{
		"t": t,				// transactionID
		"y": "e",			// error
		"e": []interface{}{ // error detail
			errCode,
			errMsg,
		},
	}
}

// send sends data to the udp.
func send(dht *DHT, addr *net.UDPAddr, data map[string]interface{}) error {
	// 超时控制
	dht.conn.SetWriteDeadline(time.Now().Add(time.Second * 15))
	// 发送 udp 报文到 addr
	_, err := dht.conn.WriteToUDP([]byte(Encode(data)), addr)
	if err != nil {
		// 出错则写入黑名单
		dht.blackList.insert(addr.IP.String(), -1)
	}
	// 返回
	return err
}

// query represents the query data included queried node and query-formed data.
type query struct {
	node *node
	data map[string]interface{}
}

// transaction implements transaction.
type transaction struct {
	*query						// query
	id       string				// transId
	response chan struct{}		// response
}

// transactionManager represents the manager of transactions.
type transactionManager struct {
	*sync.RWMutex
	transactions *syncedMap		// <tranId, trans>
	index        *syncedMap		// <queryType:address, trans>
	cursor       uint64
	maxCursor    uint64
	queryChan    chan *query
	dht          *DHT
}

// newTransactionManager returns new transactionManager pointer.
func newTransactionManager(maxCursor uint64, dht *DHT) *transactionManager {
	return &transactionManager{
		RWMutex:      &sync.RWMutex{},
		transactions: newSyncedMap(),
		index:        newSyncedMap(),
		maxCursor:    maxCursor,
		queryChan:    make(chan *query, 1024),
		dht:          dht,
	}
}

// genTransID generates a transaction id and returns it.
func (tm *transactionManager) genTransID() string {
	tm.Lock()
	defer tm.Unlock()

	tm.cursor = (tm.cursor + 1) % tm.maxCursor
	return string(int2bytes(tm.cursor))
}

// newTransaction creates a new transaction.
func (tm *transactionManager) newTransaction(id string, q *query) *transaction {
	return &transaction{
		id:       id,
		query:    q,
		response: make(chan struct{}, tm.dht.Try+1),
	}
}

// genIndexKey generates an indexed key which consists of queryType and
// address.
func (tm *transactionManager) genIndexKey(queryType, address string) string {
	// queryType:address
	return strings.Join([]string{queryType, address}, ":")
}

// genIndexKeyByTrans generates an indexed key by a transaction.
func (tm *transactionManager) genIndexKeyByTrans(trans *transaction) string {
	// queryType:address
	return tm.genIndexKey(trans.data["q"].(string), trans.node.addr.String())
}

// insert adds a transaction to transactionManager.
func (tm *transactionManager) insert(trans *transaction) {
	tm.Lock()
	defer tm.Unlock()

	// 保存 <tranId, trans> 到 map 中
	tm.transactions.Set(trans.id, trans)
	// 保存 <queryType:address, trans> 到 map 中
	tm.index.Set(tm.genIndexKeyByTrans(trans), trans)
}

// delete removes a transaction from transactionManager.
func (tm *transactionManager) delete(transID string) {

	// 根据 transId 获取 trans
	v, ok := tm.transactions.Get(transID)
	if !ok {
		return
	}

	tm.Lock()
	defer tm.Unlock()

	trans := v.(*transaction)

	// 移除 <transId, trans>
	tm.transactions.Delete(trans.id)
	// 移除 <queryType:address, trans>
	tm.index.Delete(tm.genIndexKeyByTrans(trans))
}

// len returns how many transactions are requesting now.
func (tm *transactionManager) len() int {
	return tm.transactions.Len()
}

// transaction returns a transaction.
// keyType should be one of 0, 1 which represents transId and index each.
func (tm *transactionManager) transaction(key string, keyType int) *transaction {

	sm := tm.transactions

	//
	if keyType == 1 {
		sm = tm.index
	}

	// 查询 trans
	v, ok := sm.Get(key)
	if !ok {
		return nil
	}

	return v.(*transaction)
}

// getByTransID returns a transaction by transID.
func (tm *transactionManager) getByTransID(transID string) *transaction {
	return tm.transaction(transID, 0)
}

// getByIndex returns a transaction by indexed key.
func (tm *transactionManager) getByIndex(index string) *transaction {
	return tm.transaction(index, 1)
}

// transaction gets the proper transaction with whose id is transId and
// address is addr.
func (tm *transactionManager) filterOne(transID string, addr *net.UDPAddr) *transaction {

	// 根据 transId 查询 trans
	trans := tm.getByTransID(transID)

	// 不存在，或者不匹配，返回 nil
	if trans == nil || trans.node.addr.String() != addr.String() {
		return nil
	}

	return trans
}

// query sends the query-formed data to udp and wait for the response.
// When timeout, it will retry `try - 1` times, which means it will query
// `try` times totally.
//
func (tm *transactionManager) query(q *query, try int) {
	// transId
	transID := q.data["t"].(string)
	// trans
	trans := tm.newTransaction(transID, q)

	//
	tm.insert(trans)
	defer tm.delete(trans.id)

	success := false
	for i := 0; i < try; i++ {
		if err := send(tm.dht, q.node.addr, q.data); err != nil {
			break
		}

		select {
		case <-trans.response:
			success = true
			break
		case <-time.After(time.Second * 15):
		}
	}

	if !success && q.node.id != nil {
		tm.dht.blackList.insert(q.node.addr.IP.String(), q.node.addr.Port)
		tm.dht.routingTable.RemoveByAddr(q.node.addr.String())
	}
}

// run starts to listen and consume the query chan.
func (tm *transactionManager) run() {
	var q *query
	for {
		select {
		case q = <-tm.queryChan:
			// 异步执行 Query 请求
			go tm.query(q, tm.dht.Try)
		}
	}
}

// sendQuery send query-formed data to the chan.
func (tm *transactionManager) sendQuery(no *node, queryType string, a map[string]interface{}) {
	// If the target is self, then stop.
	//
	// Cond1. 目标节点为本节点
	if no.id != nil && no.id.RawString() == tm.dht.node.id.RawString() ||
		// Cond2. 如果 queryType:address 的查询请求非空
		tm.getByIndex(tm.genIndexKey(queryType, no.addr.String())) != nil ||
		// Cond3. 如果目标 Node 的 IP/Port 在黑名单中
		tm.dht.blackList.in(no.addr.IP.String(), no.addr.Port) {
		// 直接返回
		return
	}

	// 构造查询请求
	data := makeQuery(tm.genTransID(), queryType, a)

	// 发送请求
	tm.queryChan <- &query{
		node: no,	// 目标节点
		data: data,	// 请求体
	}
}

// ping sends ping query to the chan.
func (tm *transactionManager) ping(no *node) {
	// 发送 ping 类型消息到 no 节点
	tm.sendQuery(no, pingType, map[string]interface{}{
		"id": tm.dht.id(no.id.RawString()), // ??? 本机 NodeID ???
	})
}

// findNode sends find_node query to the chan.
func (tm *transactionManager) findNode(no *node, target string) {
	// 发送 findNode 类型消息到 no 节点
	tm.sendQuery(no, findNodeType, map[string]interface{}{
		"id":     tm.dht.id(target),	// ??? 本机 NodeID ???
		"target": target,				// 查找的目标节点 NodeId
	})
}

// getPeers sends get_peers query to the chan.
//
// 发请求给 no ，根据 info_hash 查询 peers 。
func (tm *transactionManager) getPeers(no *node, infoHash string) {
	// 发送 getPeers 到 no 节点
	tm.sendQuery(no, getPeersType, map[string]interface{}{
		"id":        tm.dht.id(infoHash),
		"info_hash": infoHash,
	})
}

// announcePeer sends announce_peer query to the chan.
func (tm *transactionManager) announcePeer(
	no *node, infoHash string, impliedPort, port int, token string) {

	tm.sendQuery(no, announcePeerType, map[string]interface{}{
		"id":           tm.dht.id(no.id.RawString()),
		"info_hash":    infoHash,
		"implied_port": impliedPort,
		"port":         port,
		"token":        token,
	})
}

// ParseKey parses the key in dict data. `t` is type of the keyed value.
// It's one of "int", "string", "map", "list".
func ParseKey(data map[string]interface{}, key string, t string) error {

	// 从 data 中提取 key ，不存在则报错
	val, ok := data[key]
	if !ok {
		return errors.New("lack of key")
	}

	// 数据类型转换
	switch t {
	case "string":
		_, ok = val.(string)
	case "int":
		_, ok = val.(int)
	case "map":
		_, ok = val.(map[string]interface{})
	case "list":
		_, ok = val.([]interface{})
	default:
		panic("invalid type")
	}

	// 类型错误，报错返回
	if !ok {
		return errors.New("invalid key type")
	}

	return nil
}

// ParseKeys parses keys. It just wraps ParseKey.
//
// 检查 data 是否包含 pairs 指定的字段及类型
func ParseKeys(data map[string]interface{}, pairs [][]string) error {
	for _, args := range pairs {
		key, typ := args[0], args[1]
		// 检查 data[key] 是否是 typ 类型，如果 key 不存在或者类型错误，若报错
		if err := ParseKey(data, key, typ); err != nil {
			return err
		}
	}
	return nil
}

// parseMessage parses the basic data received from udp.
// It returns a map value.
func parseMessage(data interface{}) (map[string]interface{}, error) {
	// 检查是否为 dict 类型
	response, ok := data.(map[string]interface{})
	if !ok {
		return nil, errors.New("response is not dict")
	}
	// 确保 response 保护 string 类型的 t 和 y 字段
	if err := ParseKeys(response, [][]string{{"t", "string"}, {"y", "string"}}); err != nil {
		return nil, err
	}
	return response, nil
}

// handleRequest handles the requests received from udp.
//
// 处理请求
func handleRequest(dht *DHT, addr *net.UDPAddr, response map[string]interface{}) (success bool) {

	// transId
	t := response["t"].(string)

	// 参数校验
	if err := ParseKeys(response, [][]string{{"q", "string"}, {"a", "map"}}); err != nil {
		// 字段非法，报错返回
		send(dht, addr, makeError(t, protocolError, err.Error()))
		return
	}

	// 提取参数
	q := response["q"].(string)
	a := response["a"].(map[string]interface{})

	// 参数校验
	if err := ParseKey(a, "id", "string"); err != nil {
		send(dht, addr, makeError(t, protocolError, err.Error()))
		return
	}

	id := a["id"].(string)

	// 本机？
	if id == dht.node.id.RawString() {
		return
	}

	// 合法性检查
	if len(id) != 20 {
		send(dht, addr, makeError(t, protocolError, "invalid id"))
		return
	}

	// 路由查找，检查 id 是否匹配
	if no, ok := dht.routingTable.GetNodeByAddress(addr.String()); ok && no.id.RawString() != id {
		// 加入黑名单
		dht.blackList.insert(addr.IP.String(), addr.Port)
		// 移除路由
		dht.routingTable.RemoveByAddr(addr.String())
		send(dht, addr, makeError(t, protocolError, "invalid id"))
		return
	}

	// query type
	switch q {
	case pingType:
		// 返回本机 ID
		send(dht, addr, makeResponse(t, map[string]interface{}{
			"id": dht.id(id),
		}))
	case findNodeType:
		if dht.IsStandardMode() {

			// 提取 target 参数
			if err := ParseKey(a, "target", "string"); err != nil {
				send(dht, addr, makeError(t, protocolError, err.Error()))
				return
			}
			target := a["target"].(string)
			if len(target) != 20 {
				send(dht, addr, makeError(t, protocolError, "invalid target"))
				return
			}

			// 计算 target 对应的 Bitmap ，用于计算 XOR
			targetID := newBitmapFromString(target)

			//
			no, _ := dht.routingTable.GetNodeKBucketByID(targetID)

			var nodes string
			if no != nil {
				nodes = no.CompactNodeInfo()
			} else {
				nodes = strings.Join(
					dht.routingTable.GetNeighborCompactInfos(targetID, dht.K),
					"",
				)
			}

			//
			send(dht, addr, makeResponse(t, map[string]interface{}{
				"id":    dht.id(target),
				"nodes": nodes,
			}))

		}
	case getPeersType:
		// 提取 info_hash 参数
		if err := ParseKey(a, "info_hash", "string"); err != nil {
			send(dht, addr, makeError(t, protocolError, err.Error()))
			return
		}
		infoHash := a["info_hash"].(string)
		if len(infoHash) != 20 {
			send(dht, addr, makeError(t, protocolError, "invalid info_hash"))
			return
		}

		// 爬虫模式
		if dht.IsCrawlMode() {
			send(dht, addr, makeResponse(t, map[string]interface{}{
				"id":    dht.id(infoHash),
				"token": dht.tokenManager.token(addr),
				"nodes": "",
			}))
		} else if peers := dht.peersManager.GetPeers(infoHash, dht.K); len(peers) > 0 {
			// 把 peers 转换成 list[<ip:port>]
			values := make([]interface{}, len(peers))
			for i, p := range peers {
				values[i] = p.CompactIPPortInfo()
			}
			// 返回
			send(dht, addr, makeResponse(t, map[string]interface{}{
				"id":     dht.id(infoHash),
				"values": values,
				"token":  dht.tokenManager.token(addr),
			}))
		} else {
			// 查找距离 infoHash 最近的 k 个节点。
			neighbors := dht.routingTable.GetNeighborCompactInfos(newBitmapFromString(infoHash), dht.K)
			// 返回
			send(dht, addr, makeResponse(t, map[string]interface{}{
				"id":    dht.id(infoHash),
				"token": dht.tokenManager.token(addr),
				"nodes": strings.Join(neighbors, ""),
			}))
		}

		// 事件回调函数
		if dht.OnGetPeers != nil {
			dht.OnGetPeers(infoHash, addr.IP.String(), addr.Port)
		}

	//
	case announcePeerType:

		// 参数检查
		if err := ParseKeys(a, [][]string{
			{"info_hash", "string"},
			{"port", "int"},
			{"token", "string"}}); err != nil {
			send(dht, addr, makeError(t, protocolError, err.Error()))
			return
		}

		//
		infoHash := a["info_hash"].(string)
		port := a["port"].(int)
		token := a["token"].(string)

		if !dht.tokenManager.check(addr, token) {
			// send(dht, addr, makeError(t, protocolError, "invalid token"))
			return
		}

		if impliedPort, ok := a["implied_port"]; ok && impliedPort.(int) != 0 {
			port = addr.Port
		}

		if dht.IsStandardMode() {
			dht.peersManager.Insert(infoHash, newPeer(addr.IP, port, token))
			send(dht, addr, makeResponse(t, map[string]interface{}{
				"id": dht.id(id),
			}))
		}

		if dht.OnAnnouncePeer != nil {
			dht.OnAnnouncePeer(infoHash, addr.IP.String(), port)
		}
	default:
		//	send(dht, addr, makeError(t, protocolError, "invalid q"))
		return
	}

	no, _ := newNode(id, addr.Network(), addr.String())
	dht.routingTable.Insert(no)
	return true
}

// findOn puts nodes in the response to the routingTable, then if target is in
// the nodes or all nodes are in the routingTable, it stops. Otherwise it
// continues to findNode or getPeers.
//
// findOn 将节点放在 routingTable 的响应中，如果目标在节点中或者所有节点都在routingTable中，就停止。
// 否则继续 findNode 或 getPeers 。
//
//
func findOn(dht *DHT, r map[string]interface{}, target *bitmap, queryType string) error {
	// 从 r 中提取 nodes 参数，它包含若干 nodeInfos ，每个 nodeInfo 是 26 位字符串，所有 nodes 需要是 26 的整数倍。
	if err := ParseKey(r, "nodes", "string"); err != nil {
		return err
	}
	nodes := r["nodes"].(string)
	if len(nodes)%26 != 0 {
		return errors.New("the length of nodes should can be divided by 26")
	}

	hasNew, found := false, false

	// 解析并遍历 nodeInfos
	for i := 0; i < len(nodes)/26; i++ {

		// 提取 nodeInfo ，它是 26 位字符串:
		//	1) nodeInfo[ 0:20] => node id
		//  2) nodeInfo[20:26] => addr(ip, port)
		nodeInfo := string(nodes[i*26:(i+1)*26])

		// 基于 nodeInfo 创建 *node
		no, _ := newNodeFromCompactInfo(nodeInfo, dht.Network)

		// 如果返回的 nodeId 恰好为查询的 target ，设置 found ~
		if no.id.RawString() == target.RawString() {
			found = true
		}

		// 将 *node 插入到路由表，如果是新增节点，设置 hasNew ～
		if dht.routingTable.Insert(no) {
			hasNew = true
		}
	}

	// 如果成功查找到 target ，或者所有节点均已存在于路由表中，就直接返回。
	if found || !hasNew {
		return nil
	}

	// 至此，意味着未成功查找到 target ，需要继续查询。

	targetID := target.RawString()
	// 返回距离 targetID 最近的(最多) K 个 *node
	for _, no := range dht.routingTable.GetNeighbors(target, dht.K) {
		switch queryType {
		case findNodeType:
			dht.transactionManager.findNode(no, targetID)
		case getPeersType:
			dht.transactionManager.getPeers(no, targetID)
		default:
			panic("invalid find type")
		}
	}

	return nil
}

// handleResponse handles responses received from udp.
func handleResponse(dht *DHT, addr *net.UDPAddr, response map[string]interface{}) (success bool) {
	// transId
	t := response["t"].(string)

	// trans
	trans := dht.transactionManager.filterOne(t, addr)
	if trans == nil {
		return
	}

	// inform transManager to delete the transaction.
	//
	// 参数校验：检测 response[r] 是否存在
	if err := ParseKey(response, "r", "map"); err != nil {
		return
	}

	// query type
	q := trans.data["q"].(string)
	// query params
	a := trans.data["a"].(map[string]interface{})

	// response params
	r := response["r"].(map[string]interface{})
	if err := ParseKey(r, "id", "string"); err != nil {
		return
	}

	// response node id
	id := r["id"].(string)

	// If response's node id is not the same with the node id in the transaction, raise error.
	//
	// 检查 nodeId 是否和 response[r].id 相匹配，如果不匹配，则回包者可能有问题，需要把回包者加入黑名单，并将其从路由表中移除。
	if trans.node.id != nil && trans.node.id.RawString() != r["id"].(string) {
		dht.blackList.insert(addr.IP.String(), addr.Port)
		dht.routingTable.RemoveByAddr(addr.String())
		return
	}


	// 构造 response 节点
	node, err := newNode(id, addr.Network(), addr.String())
	if err != nil {
		return
	}

	// 根据 query type 做对应处理
	switch q {
	case pingType:
	case findNodeType:
		// ignore, 没啥用
		if trans.data["q"].(string) != findNodeType {
			return
		}

		// 从查询参数中提取 target
		target := trans.data["a"].(map[string]interface{})["target"].(string)

		//
		if findOn(dht, r, newBitmapFromString(target), findNodeType) != nil {
			return
		}

	// 根据
	case getPeersType:

		// 从 resp.Params 中提取 token 参数
		if err := ParseKey(r, "token", "string"); err != nil {
			return
		}
		token := r["token"].(string)

		// 从 req.Params 中提取 info_hash 参数
		infoHash := a["info_hash"].(string)

		// 从 resp.Params 中提取 values 参数
		if err := ParseKey(r, "values", "list"); err == nil {
			values := r["values"].([]interface{})
			for _, v := range values {

				// v 即为 "IP:Port" ，这里创建 *peer
				p, err := newPeerFromCompactIPPortInfo(v.(string), token)
				if err != nil {
					continue
				}

				// 把 <infoHash, *peer> 插入到 dht 中
				dht.peersManager.Insert(infoHash, p)

				// 回调函数
				if dht.OnGetPeersResponse != nil {
					dht.OnGetPeersResponse(infoHash, p)
				}
			}
		//
		} else if findOn(dht, r, newBitmapFromString(infoHash), getPeersType) != nil {
			return
		}

	case announcePeerType:
	default:
		return
	}

	// inform transManager to delete transaction.
	trans.response <- struct{}{}

	dht.blackList.delete(addr.IP.String(), addr.Port)
	dht.routingTable.Insert(node)

	return true
}

// handleError handles errors received from udp.
func handleError(dht *DHT, addr *net.UDPAddr, response map[string]interface{}) (success bool) {

	if err := ParseKey(response, "e", "list"); err != nil {
		return
	}

	if e := response["e"].([]interface{}); len(e) != 2 {
		return
	}

	if trans := dht.transactionManager.filterOne(response["t"].(string), addr); trans != nil {
		trans.response <- struct{}{}
	}

	return true
}

var handlers = map[string]func(*DHT, *net.UDPAddr, map[string]interface{}) bool{
	"q": handleRequest,	// request
	"r": handleResponse,// response
	"e": handleError,	// error
}

// handle handles packets received from udp.
//
// 处理 Udp Packet
func handle(dht *DHT, pkt packet) {
	if len(dht.workerTokens) == dht.PacketWorkerLimit {
		return
	}

	// 并发控制：获取令牌
	dht.workerTokens <- struct{}{}
	go func() {
		// 并发控制：释放令牌
		defer func() {
			<-dht.workerTokens
		}()

		// 来自黑名单，则直接返回
		if dht.blackList.in(pkt.raddr.IP.String(), pkt.raddr.Port) {
			return
		}

		// 解析 BT 协议
		data, err := Decode(pkt.data)
		if err != nil {
			return
		}

		// 解析 Dict
		response, err := parseMessage(data)
		if err != nil {
			return
		}

		// 获取 handler 并执行
		if f, ok := handlers[response["y"].(string)]; ok {
			f(dht, pkt.raddr, response)
		}
	}()
}
