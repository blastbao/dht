package dht

import (
	"errors"
	"net"
	"strings"
	"sync"
	"time"
)

const (
	pingType         = "ping"
	findNodeType     = "find_node"
	getPeersType     = "get_peers"
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
		"a": a,		// params
	}
}

// makeResponse returns a response-formed data.
//
// 构造响应
func makeResponse(t string, r map[string]interface{}) map[string]interface{} {
	return map[string]interface{}{
		"t": t,		// transactionID
		"y": "r",	// response
		"r": r,		// params
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
	*query
	id       string
	response chan struct{}
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

	if keyType == 1 {
		sm = tm.index
	}

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
func (tm *transactionManager) filterOne(
	transID string, addr *net.UDPAddr) *transaction {

	trans := tm.getByTransID(transID)
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

		// 参数检查
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
func findOn(dht *DHT, r map[string]interface{}, target *bitmap, queryType string) error {

	if err := ParseKey(r, "nodes", "string"); err != nil {
		return err
	}

	nodes := r["nodes"].(string)
	if len(nodes)%26 != 0 {
		return errors.New("the length of nodes should can be divided by 26")
	}

	hasNew, found := false, false
	for i := 0; i < len(nodes)/26; i++ {
		no, _ := newNodeFromCompactInfo(
			string(nodes[i*26:(i+1)*26]), dht.Network)

		if no.id.RawString() == target.RawString() {
			found = true
		}

		if dht.routingTable.Insert(no) {
			hasNew = true
		}
	}

	if found || !hasNew {
		return nil
	}

	targetID := target.RawString()
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

	//
	trans := dht.transactionManager.filterOne(t, addr)
	if trans == nil {
		return
	}

	// inform transManager to delete the transaction.
	if err := ParseKey(response, "r", "map"); err != nil {
		return
	}

	q := trans.data["q"].(string)
	a := trans.data["a"].(map[string]interface{})
	r := response["r"].(map[string]interface{})

	if err := ParseKey(r, "id", "string"); err != nil {
		return
	}

	id := r["id"].(string)

	// If response's node id is not the same with the node id in the
	// transaction, raise error.
	if trans.node.id != nil && trans.node.id.RawString() != r["id"].(string) {
		dht.blackList.insert(addr.IP.String(), addr.Port)
		dht.routingTable.RemoveByAddr(addr.String())
		return
	}

	node, err := newNode(id, addr.Network(), addr.String())
	if err != nil {
		return
	}

	switch q {
	case pingType:
	case findNodeType:
		if trans.data["q"].(string) != findNodeType {
			return
		}

		target := trans.data["a"].(map[string]interface{})["target"].(string)
		if findOn(dht, r, newBitmapFromString(target), findNodeType) != nil {
			return
		}
	case getPeersType:
		if err := ParseKey(r, "token", "string"); err != nil {
			return
		}

		token := r["token"].(string)
		infoHash := a["info_hash"].(string)

		if err := ParseKey(r, "values", "list"); err == nil {
			values := r["values"].([]interface{})
			for _, v := range values {
				p, err := newPeerFromCompactIPPortInfo(v.(string), token)
				if err != nil {
					continue
				}
				dht.peersManager.Insert(infoHash, p)
				if dht.OnGetPeersResponse != nil {
					dht.OnGetPeersResponse(infoHash, p)
				}
			}
		} else if findOn(
			dht, r, newBitmapFromString(infoHash), getPeersType) != nil {
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
func handleError(dht *DHT, addr *net.UDPAddr,
	response map[string]interface{}) (success bool) {

	if err := ParseKey(response, "e", "list"); err != nil {
		return
	}

	if e := response["e"].([]interface{}); len(e) != 2 {
		return
	}

	if trans := dht.transactionManager.filterOne(
		response["t"].(string), addr); trans != nil {

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
