package dht

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"io"
	"io/ioutil"
	"net"
	"strings"
	"time"
)

const (
	// REQUEST represents request message type
	REQUEST = iota
	// DATA represents data message type
	DATA
	// REJECT represents reject message type
	REJECT
)

const (
	// BLOCK is 2 ^ 14
	BLOCK = 16384
	// MaxMetadataSize represents the max medata it can accept
	MaxMetadataSize = BLOCK * 1000
	// EXTENDED represents it is a extended message
	EXTENDED = 20
	// HANDSHAKE represents handshake bit
	HANDSHAKE = 0
)

// 握手包前缀
var handshakePrefix = []byte{
	19,   66, 105, 116, 84,
	111, 114, 114, 101, 110,
	116,  32, 112, 114, 111,
	116, 111,  99, 111, 108,
	  0,   0,  0,    0,   0,
	 16,   0,  1,
}

// read reads size-length bytes from conn to data.
func read(conn *net.TCPConn, size int, data *bytes.Buffer) error {
	// 超时 15s
	conn.SetReadDeadline(time.Now().Add(time.Second * 15))

	// 从 conn 读取 size Bytes 数据到 data 中
	n, err := io.CopyN(data, conn, int64(size))
	if err != nil || n != int64(size) {
		return errors.New("read error")
	}
	return nil
}

// readMessage gets a message from the tcp connection.
func readMessage(conn *net.TCPConn, data *bytes.Buffer) (length int, err error) {
	// 读取 length
	if err = read(conn, 4, data); err != nil {
		return
	}

	// 解析 length
	length = int(bytes2int(data.Next(4)))
	if length == 0 {
		return
	}

	// 读取 body
	if err = read(conn, length, data); err != nil {
		return
	}
	return
}

// sendMessage sends data to the connection.
func sendMessage(conn *net.TCPConn, data []byte) error {
	length := int32(len(data))

	// 发送 length
	buffer := bytes.NewBuffer(nil)
	binary.Write(buffer, binary.BigEndian, length)

	// 发送 data
	conn.SetWriteDeadline(time.Now().Add(time.Second * 10))
	_, err := conn.Write(append(buffer.Bytes(), data...))
	return err
}

// sendHandshake sends handshake message to conn.
func sendHandshake(conn *net.TCPConn, infoHash, peerID []byte) error {
	// 构造握手包
	data := make([]byte, 68)
	copy(data[:28], handshakePrefix)	// 握手包前缀
	copy(data[28:48], infoHash)			// hash
	copy(data[48:], peerID)				// peer id

	// 发送握手包
	conn.SetWriteDeadline(time.Now().Add(time.Second * 10))
	_, err := conn.Write(data)
	return err
}

// onHandshake handles the handshake response.
func onHandshake(data []byte) (err error) {
	// 检查握手包前缀
	if !(bytes.Equal(handshakePrefix[:20], data[:20]) && data[25]&0x10 != 0) {
		err = errors.New("invalid handshake response")
	}
	return
}

// sendExtHandshake requests for the ut_metadata and metadata_size.
//
// 发送扩展消息
func sendExtHandshake(conn *net.TCPConn) error {
	data := append(
		[]byte{ EXTENDED, HANDSHAKE },
		Encode(map[string]interface{}{
			"m": map[string]interface{}{"ut_metadata": 1},
		})...,
	)
	return sendMessage(conn, data)
}

// getUTMetaSize returns the ut_metadata and metadata_size.
func getUTMetaSize(data []byte) (utMetadata int, metadataSize int, err error) {

	// BT 协议解析
	v, err := Decode(data)
	if err != nil {
		return
	}

	dict, ok := v.(map[string]interface{})
	if !ok {
		err = errors.New("invalid dict")
		return
	}

	// 参数校验
	if err = ParseKeys(
		dict, [][]string{{"metadata_size", "int"}, {"m", "map"}}); err != nil {
		return
	}

	m := dict["m"].(map[string]interface{})
	if err = ParseKey(m, "ut_metadata", "int"); err != nil {
		return
	}

	utMetadata = m["ut_metadata"].(int)
	metadataSize = dict["metadata_size"].(int)

	if metadataSize > MaxMetadataSize {
		err = errors.New("metadata_size too long")
	}

	return
}

// Request represents the request context.
type Request struct {
	InfoHash []byte
	IP       string
	Port     int
}

// Response contains the request context and the metadata info.
type Response struct {
	Request
	MetadataInfo []byte
}

// Wire represents the wire protocol.
type Wire struct {
	blackList    *blackList
	queue        *syncedMap
	requests     chan Request
	responses    chan Response
	workerTokens chan struct{}
}

// NewWire returns a Wire pointer.
//   - blackListSize: the blacklist size
//   - requestQueueSize: the max requests it can buffers
//   - workerQueueSize: the max goroutine downloading workers
func NewWire(blackListSize, requestQueueSize, workerQueueSize int) *Wire {
	return &Wire{
		blackList:    newBlackList(blackListSize),
		queue:        newSyncedMap(),
		requests:     make(chan Request, requestQueueSize),
		responses:    make(chan Response, 1024),
		workerTokens: make(chan struct{}, workerQueueSize),
	}
}

// Request pushes the request to the queue.
func (wire *Wire) Request(infoHash []byte, ip string, port int) {
	wire.requests <- Request{InfoHash: infoHash, IP: ip, Port: port}
}

// Response returns a chan of Response.
func (wire *Wire) Response() <-chan Response {
	return wire.responses
}

// isDone returns whether the wire get all pieces of the metadata info.
func (wire *Wire) isDone(pieces [][]byte) bool {
	for _, piece := range pieces {
		if len(piece) == 0 {
			return false
		}
	}
	return true
}

func (wire *Wire) requestPieces(
	conn *net.TCPConn, utMetadata int, metadataSize int, piecesNum int) {

	buffer := make([]byte, 1024)
	for i := 0; i < piecesNum; i++ {
		buffer[0] = EXTENDED
		buffer[1] = byte(utMetadata)

		msg := Encode(map[string]interface{}{
			"msg_type": REQUEST,
			"piece":    i,
		})

		length := len(msg) + 2
		copy(buffer[2:length], msg)

		sendMessage(conn, buffer[:length])
	}
	buffer = nil
}

// fetchMetadata fetchs medata info accroding to infohash from dht.
func (wire *Wire) fetchMetadata(r Request) {
	var (
		length       int
		msgType      byte
		piecesNum    int
		pieces       [][]byte
		utMetadata   int
		metadataSize int
	)

	defer func() {
		pieces = nil
		recover()
	}()

	// parameters
	infoHash := r.InfoHash
	address := genAddress(r.IP, r.Port)

	// tcp connection
	dial, err := net.DialTimeout("tcp", address, time.Second*15)
	if err != nil {
		wire.blackList.insert(r.IP, r.Port)
		return
	}
	conn := dial.(*net.TCPConn)
	conn.SetLinger(0)
	defer conn.Close()

	// 数据缓存
	data := bytes.NewBuffer(nil)
	data.Grow(BLOCK)

	// 发送握手包
	if sendHandshake(conn, infoHash, []byte(randomString(20))) != nil ||
		// 读取响应包
		read(conn, 68, data) != nil ||
		// 执行握手
		onHandshake(data.Next(68)) != nil ||
		// 发送扩展包
		sendExtHandshake(conn) != nil {
		return
	}

	for {
		// 读取数据
		length, err = readMessage(conn, data)
		if err != nil {
			return
		}
		// 空数据
		if length == 0 {
			continue
		}

		// 读取 1B 的 msgType
		msgType, err = data.ReadByte()
		if err != nil {
			return
		}

		//
		switch msgType {
		case EXTENDED:
			extendedID, err := data.ReadByte()
			if err != nil {
				return
			}

			payload, err := ioutil.ReadAll(data)
			if err != nil {
				return
			}

			if extendedID == 0 {
				if pieces != nil {
					return
				}

				utMetadata, metadataSize, err = getUTMetaSize(payload)
				if err != nil {
					return
				}

				piecesNum = metadataSize / BLOCK
				if metadataSize%BLOCK != 0 {
					piecesNum++
				}

				pieces = make([][]byte, piecesNum)
				go wire.requestPieces(conn, utMetadata, metadataSize, piecesNum)

				continue
			}

			if pieces == nil {
				return
			}

			d, index, err := DecodeDict(payload, 0)
			if err != nil {
				return
			}
			dict := d.(map[string]interface{})

			if err = ParseKeys(dict, [][]string{
				{"msg_type", "int"},
				{"piece", "int"}}); err != nil {
				return
			}

			if dict["msg_type"].(int) != DATA {
				continue
			}

			piece := dict["piece"].(int)
			pieceLen := length - 2 - index

			if (piece != piecesNum-1 && pieceLen != BLOCK) ||
			   (piece == piecesNum-1 && pieceLen != metadataSize%BLOCK) {
				return
			}

			pieces[piece] = payload[index:]

			if wire.isDone(pieces) {
				metadataInfo := bytes.Join(pieces, nil)

				info := sha1.Sum(metadataInfo)
				if !bytes.Equal(infoHash, info[:]) {
					return
				}

				wire.responses <- Response{
					Request:      r,
					MetadataInfo: metadataInfo,
				}
				return
			}
		default:
			data.Reset()
		}
	}
}

// Run starts the peer wire protocol.
func (wire *Wire) Run() {

	// 每隔 10 min 移除黑名单中的过期条目
	go wire.blackList.clear()

	for r := range wire.requests {
		// 并发控制
		wire.workerTokens <- struct{}{}
		go func(r Request) {
			defer func() {
				// 并发控制
				<-wire.workerTokens
			}()

			// key = infoHash:ip:port
			key := strings.Join([]string{
				string(r.InfoHash), genAddress(r.IP, r.Port),
			}, ":")

			// 参数非法 || IP:Port 非法 || 请求重复
			if len(r.InfoHash) != 20 || wire.blackList.in(r.IP, r.Port) || wire.queue.Has(key) {
				// 拒绝
				return
			}

			// 拉取元数据
			wire.fetchMetadata(r)
		}(r)
	}
}
