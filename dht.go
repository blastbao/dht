// Package dht implements the bittorrent dht protocol. For more information
// see http://www.bittorrent.org/beps/bep_0005.html.
package dht

import (
	"encoding/hex"
	"errors"
	"math"
	"net"
	"time"
)

const (
	// StandardMode follows the standard protocol
	StandardMode = iota
	// CrawlMode for crawling the dht network.
	CrawlMode
)

var (
	// ErrNotReady is the error when DHT is not initialized.
	ErrNotReady = errors.New("dht is not ready")
	// ErrOnGetPeersResponseNotSet is the error that config
	// OnGetPeersResponseNotSet is not set when call dht.GetPeers.
	ErrOnGetPeersResponseNotSet = errors.New("OnGetPeersResponse is not set")
)

// Config represents the configure of dht.
type Config struct {

	// in mainline dht, k = 8
	K int

	// for crawling mode, we put all nodes in one bucket, so KBucketSize may
	// not be K
	KBucketSize int

	// candidates are udp, udp4, udp6
	Network string

	// format is `ip:port`
	Address string

	// the prime nodes through which we can join in dht network
	//
	// boost 节点
	PrimeNodes []string

	// the kbucket expired duration
	//
	// k-bucket 过期时间
	KBucketExpiredAfter time.Duration

	// the node expired duration
	//
	// 节点过期时间
	NodeExpriedAfter time.Duration

	// how long it checks whether the bucket is expired
	CheckKBucketPeriod time.Duration

	// peer token expired duration
	TokenExpiredAfter time.Duration

	// the max transaction id
	MaxTransactionCursor uint64

	// how many nodes routing table can hold
	MaxNodes int
	// callback when got get_peers request
	OnGetPeers func(string, string, int)
	// callback when receive get_peers response
	OnGetPeersResponse func(string, *Peer)
	// callback when got announce_peer request
	OnAnnouncePeer func(string, string, int)
	// blcoked ips
	BlockedIPs []string
	// blacklist size
	BlackListMaxSize int
	// StandardMode or CrawlMode
	Mode int
	// the times it tries when send fails
	Try int
	// the size of packet need to be dealt with
	PacketJobLimit int
	// the size of packet handler
	PacketWorkerLimit int
	// the nodes num to be fresh in a kbucket
	RefreshNodeNum int
}

// NewStandardConfig returns a Config pointer with default values.
func NewStandardConfig() *Config {
	return &Config{
		K:           8,
		KBucketSize: 8,
		Network:     "udp4",
		Address:     ":6881",
		PrimeNodes: []string{
			"router.bittorrent.com:6881",
			"router.utorrent.com:6881",
			"dht.transmissionbt.com:6881",
		},
		NodeExpriedAfter:     time.Duration(time.Minute * 15),
		KBucketExpiredAfter:  time.Duration(time.Minute * 15),
		CheckKBucketPeriod:   time.Duration(time.Second * 30),
		TokenExpiredAfter:    time.Duration(time.Minute * 10),
		MaxTransactionCursor: math.MaxUint32,
		MaxNodes:             5000,
		BlockedIPs:           make([]string, 0),
		BlackListMaxSize:     65536,
		Try:                  2,
		Mode:                 StandardMode,
		PacketJobLimit:       1024,
		PacketWorkerLimit:    256,
		RefreshNodeNum:       8,
	}
}

// NewCrawlConfig returns a config in crawling mode.
func NewCrawlConfig() *Config {
	config := NewStandardConfig()
	config.NodeExpriedAfter = 0
	config.KBucketExpiredAfter = 0
	config.CheckKBucketPeriod = time.Second * 5
	config.KBucketSize = math.MaxInt32
	config.Mode = CrawlMode
	config.RefreshNodeNum = 256

	return config
}

// DHT represents a DHT node.
type DHT struct {
	*Config
	node               *node				// 本机 Node
	conn               *net.UDPConn			// 本地 Udp Socket
	routingTable       *routingTable		// 路由表
	transactionManager *transactionManager	// 查询(事务)管理器
	peersManager       *peersManager		//
	tokenManager       *tokenManager		//
	blackList          *blackList			//
	Ready              bool					//
	packets            chan packet			// 接收 UDP Packet
	workerTokens       chan struct{}		//
}

// New returns a DHT pointer. If config is nil, then config will be set to
// the default config.
func New(config *Config) *DHT {
	if config == nil {
		config = NewStandardConfig()
	}

	// 网络节点
	node, err := newNode(randomString(20), config.Network, config.Address)
	if err != nil {
		panic(err)
	}

	d := &DHT{
		Config:       config,
		node:         node,
		blackList:    newBlackList(config.BlackListMaxSize),
		packets:      make(chan packet, config.PacketJobLimit),
		workerTokens: make(chan struct{}, config.PacketWorkerLimit),
	}

	// IP 黑名单
	for _, ip := range config.BlockedIPs {
		d.blackList.insert(ip, -1)
	}

	go func() {
		// 获取本地 IP 地址，添加到黑名单
		for _, ip := range getLocalIPs() {
			d.blackList.insert(ip, -1)
		}
		ip, err := getRemoteIP()
		if err != nil {
			d.blackList.insert(ip, -1)
		}
	}()

	return d
}

// IsStandardMode returns whether mode is StandardMode.
func (dht *DHT) IsStandardMode() bool {
	return dht.Mode == StandardMode
}

// IsCrawlMode returns whether mode is CrawlMode.
func (dht *DHT) IsCrawlMode() bool {
	return dht.Mode == CrawlMode
}

// init initializes global varables.
func (dht *DHT) init() {

	// 监听本地地址，本函数用于监听 ip、udp、unix（DGRAM）等协议，返回一个 PacketConn 接口。
	// 根据协议不同，本接口可能返回 net.IPCon、net.UDPConn、net.UnixConn 等，但它们都实现了 PacketConn 接口。
	listener, err := net.ListenPacket(dht.Network, dht.Address)
	if err != nil {
		panic(err)
	}

	// 提取连接
	dht.conn = listener.(*net.UDPConn)

	// 初始化路由表
	dht.routingTable = newRoutingTable(dht.KBucketSize, dht)

	// 连接管理器
	dht.peersManager = newPeersManager(dht)

	// 口令管理器
	dht.tokenManager = newTokenManager(dht.TokenExpiredAfter, dht)

	// 事务管理器
	dht.transactionManager = newTransactionManager(
		dht.MaxTransactionCursor,
		dht,
	)

	// 启动各个管理器
	go dht.transactionManager.run()	// 异步执行 Query 请求
	go dht.tokenManager.clear() 	// 每隔 3 min 移除 Token 表中的过期条目
	go dht.blackList.clear() 		// 每隔 10 min 移除黑名单中的过期条目
}

// join makes current node join the dht network.
func (dht *DHT) join() {
	// 遍历主节点
	for _, addr := range dht.PrimeNodes {
		// 解析 UDP 地址
		raddr, err := net.ResolveUDPAddr(dht.Network, addr)
		if err != nil {
			continue
		}
		// NOTE: Temporary node has NOT node id.
		//
		// 通过执行 findNode 到 prime nodes ，让它们感知到本节点上线。
		dht.transactionManager.findNode(
			&node{						// 目标 Node
				addr: raddr,
			},
			dht.node.id.RawString(), 	// 本机 NodeID
		)
	}
}

// listen receives message from udp.
func (dht *DHT) listen() {
	go func() {
		buff := make([]byte, 8192)
		for {
			// 从 Udp Socket 中读取 Packet
			n, raddr, err := dht.conn.ReadFromUDP(buff)
			if err != nil {
				continue
			}
			// 将 Udp Packet 写入到管道中
			dht.packets <- packet{
				buff[:n],	// 数据
				raddr,	// 地址
			}
		}
	}()
}

// id returns a id near to target if target is not null, otherwise it returns
// the dht's node id.
//
//
//
func (dht *DHT) id(target string) string {
	// 返回本机 NodeId
	if dht.IsStandardMode() || target == "" {
		return dht.node.id.RawString()
	}

	//
	return target[:15] + dht.node.id.RawString()[15:]
}

// GetPeers returns peers who have announced having infoHash.
//
// 根据 infoHash 查找 peers
func (dht *DHT) GetPeers(infoHash string) error {
	if !dht.Ready {
		return ErrNotReady
	}

	if dht.OnGetPeersResponse == nil {
		return ErrOnGetPeersResponseNotSet
	}

	if len(infoHash) == 40 {
		// 将 16 进制字符串转换原始字节编码
		data, err := hex.DecodeString(infoHash)
		if err != nil {
			return err
		}
		infoHash = string(data)
	}

	// 查找路由表，获取和 hashInfo 距离最近的 n 个 neighbors
	neighbors := dht.routingTable.GetNeighbors(
		newBitmapFromString(infoHash),
		dht.routingTable.Len(),
	)

	// 发送请求给 neighbors ，查询 infoHash 的存储节点
	for _, no := range neighbors {
		dht.transactionManager.getPeers(no, infoHash)
	}

	return nil
}

// Run starts the dht.
func (dht *DHT) Run() {
	dht.init()
	dht.listen()
	dht.join()

	dht.Ready = true

	var pkt packet
	tick := time.Tick(dht.CheckKBucketPeriod)

	for {
		select {
		// 接收并处理 Udp Packet
		case pkt = <-dht.packets:
			handle(dht, pkt)
		// 定时更新路由表
		case <-tick:
			if dht.routingTable.Len() == 0 {
				dht.join()
			} else if dht.transactionManager.len() == 0 {
				go dht.routingTable.Fresh()
			}
		}
	}
}
