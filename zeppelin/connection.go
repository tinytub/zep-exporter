package zeppelin

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"

	logging "github.com/op/go-logging"
	"github.com/tinytub/zep-exporter/proto/ZPMeta"
	"github.com/tinytub/zep-exporter/proto/client"

	"github.com/golang/protobuf/proto"
)

var logger = logging.MustGetLogger("zeppelinecore")

const (
	RefreshConnInterval   = 5 * time.Second
	UselessConnChanLen    = 200
	UselessConnRemoveWait = 60 * time.Second
	maxConnNum            = 1
	connQueue             = 10
)

type Connection struct {
	Conn      net.Conn
	Addr      string
	Data      chan []byte
	ok        bool
	timestamp time.Time
	//	Mynum      int
	Mytype     string
	HasRequest chan bool
	RecvDone   chan bool
	mu         sync.Mutex
}

type TcpConns struct {
	Addrs []string
	Conns map[string](chan *Connection)
	lock  sync.RWMutex
}

var (
	connections  map[string]*TcpConns
	uselessConns chan *Connection
)

//TODO:连接刷新和保活

func InitMetaConns(addrs []string) {
	connections = make(map[string]*TcpConns)
	connections["meta"] = &TcpConns{Addrs: []string{}, Conns: make(map[string](chan *Connection))}
	// 地址从config里读
	connections["meta"].Addrs = addrs
	var wg sync.WaitGroup
	for _, addr := range connections["meta"].Addrs {
		connections["meta"].Conns[addr] = make(chan *Connection, connQueue)
		for i := 0; i < maxConnNum; i++ {
			go func(addr string) {
				wg.Add(1)
				c := &Connection{}
				err := c.newConn(addr)
				if err != nil {
					logger.Error("bad conn, continue")
					wg.Done()
					return
					//continue
				}

				c.Addr = addr
				c.Mytype = "meta"
				c.ok = true
				c.timestamp = time.Now()

				connections["meta"].lock.Lock()
				defer connections["meta"].lock.Unlock()
				connections["meta"].Conns[addr] <- c
				logger.Info("inited meta conns", addr)
				wg.Done()
				//go c.Recv()
			}(addr)
		}
	}
	logger.Info("inited meta conns")
	wg.Wait()
}

func InitNodeConns() {
	nodes, err := ListNode()
	if err != nil {
		logger.Error("can not get node list, initNodeConn err", err)
		os.Exit(1)
	}
	connections["node"] = &TcpConns{Addrs: []string{}, Conns: make(map[string](chan *Connection))}

	var wg sync.WaitGroup
	for _, node := range nodes {
		if *node.Status != int32(1) {
			nodeinfo := node.GetNode()
			addr := nodeinfo.GetIp() + ":" + strconv.Itoa(int(nodeinfo.GetPort()))
			connections["node"].Conns[addr] = make(chan *Connection, connQueue)

			for i := 0; i < maxConnNum; i++ {
				go func(addr string) {
					wg.Add(1)
					c := &Connection{}
					err := c.newConn(addr)
					if err != nil {
						logger.Warning("bad conn, continue")
						wg.Done()
						return
						//continue
					}

					c.Addr = addr
					c.Mytype = "node"
					c.ok = true
					c.timestamp = time.Now()

					connections["node"].lock.Lock()
					defer connections["node"].lock.Unlock()
					connections["node"].Conns[addr] <- c
					//go c.Recv()
					logger.Info("inited node conns", addr)
					wg.Done()
				}(addr)
			}
		}
	}
	wg.Wait()
	logger.Info("inited all node conns")

}

func (c *Connection) newConn(addr string) error {
	conn, err := net.DialTimeout("tcp", addr, 1000*time.Millisecond)
	if err != nil {
		logger.Error("tcp conn err:", err)
		return err
	}
	c.Data = make(chan []byte, 4)
	c.RecvDone = make(chan bool)
	c.HasRequest = make(chan bool)
	c.Conn = conn
	c.Addr = addr
	return nil
}

func RefreashConns() {
	uselessConns = make(chan *Connection, UselessConnChanLen)
	go removeConns()
	for {
		select {
		case <-time.After(RefreshConnInterval):
			logger.Info("time to refreash")
			refreshConns()
		}
	}

}

func removeConns() {
	for {
		select {
		case conn := <-uselessConns:
			conn.Conn.Close()
			logger.Info("conn real close")
		}
	}
}

/*
func CloseConns(conn *Connection) {
	logger.Info("close conn")
	//connections[conn.Mytype].lock.Lock()
	//defer connections[conn.Mytype].lock.Unlock()

	go func(useless *Connection) {
		time.Sleep(UselessConnRemoveWait)
		uselessConns <- useless
	}(conn)
	logger.Info("useless channel append")

	for i, c := range connections[conn.Mytype].Conns[conn.Addr] {
		if c == conn {
			fmt.Println(conn.Mytype, conn.Addr)

			connections[conn.Mytype].Conns[conn.Addr] = append(connections[conn.Mytype].Conns[conn.Addr][:i], connections[conn.Mytype].Conns[conn.Addr][i+1:]...)
			//connections[conn.Mytype].Conns[conn.Addr] = append(connections[conn.Mytype].Conns[conn.Addr][:conn.Mynum], connections[conn.Mytype].Conns[conn.Addr][conn.Mynum+1:]...)
		}
	}
	logger.Info("close done")

}
*/
func CloseAllConn() {
	time.Sleep(10 * time.Second)
	for _, tconns := range connections {
		for _, conns := range tconns.Conns {
			//这里可能塞多了。。。
			for conn := range conns {
				uselessConns <- conn
				if len(conns) == 0 {
					break
				}
			}
		}
	}
}

//TODO 改成在timeout哪里刷新？
//TODO 添加刷新meta地址和conn地址
func refreshConns() {
	for cType, tconns := range connections {
		for addr, conns := range tconns.Conns {
			//这里可能塞多了。。。
			if len(conns) < maxConnNum {
				for i := 0; i < maxConnNum-len(conns); i++ {
					c := &Connection{}
					err := c.newConn(addr)
					if err != nil {
						logger.Warning("bad conn, continue")
						continue
					}
					c.Addr = addr
					c.Mytype = cType
					c.timestamp = time.Now()
					connections[cType].Conns[addr] <- c
					logger.Info("refresh conn")
				}
			}
		}
	}
}

/*
func refreshConnsByConf() {
	uselessConns = make(chan []*Connection, UselessConnChanLen)
	go removeConns()
	for {
		select {
		case <-time.After(RefreshConnInterval):
			refreshConns()
		}
	}
}

func closeConns() {

}

func refreshConns() {
	for cType, conn := range connections {
		oldconn

	}
}

func removeConns() {
	for {
		select {
		case conns := <-uselessConns:
			for _, c := range conns {
				if c != nil {
					c.Conn.Close()
				}
			}
		}
	}
}
*/

/*
func closeConns(gconns *grpcConns, addr string) {
	gconns.lock.Lock()
	defer gconns.lock.Unlock()
	go func(useless []*grpc.ClientConn) {
		time.Sleep(UselessConnRemoveWait)
		uselessConns <- useless
	}(gconns.conns[addr])
	delete(gconns.conns, addr)
	for i, v := range gconns.addrs {
		if v == addr {
			gconns.addrs = append(gconns.addrs[:i], gconns.addrs[i+1:]...)
		}
	}
}

func refreshConns() {
	for bizName, gconns := range connections {
		oldAddrs := gconns.addrs
		newAddrs := connGetConf("Grpc"+gconns.info.BizName+"Addrs", []string{}).([]string)
		if len(newAddrs) == 0 {
			continue
		}
		delAddrs, addAddrs := utils.DiffStrSlices(oldAddrs, newAddrs)
		for _, addr := range delAddrs {
			log.Infof("close old conn, biz_name=%s, addr=%s", bizName, addr)
			closeConns(gconns, addr)
		}
		num := connGetConf("Grpc"+gconns.info.BizName+"Conns", 1).(int)
		opts, err := getDialOptions(gconns.info)
		if err != nil {
			log.Errorf("grpc getDialOptions err(%v)", err)
			continue
		}
		for _, addr := range addAddrs {
			log.Infof("open new conn, biz_name=%s, addr=%s", bizName, addr)
			openConns(gconns, addr, num, opts)
		}
	}
}

func removeConns() {
	for {
		select {
		case conns := <-uselessConns:
			log.Info("close grpc client conns")
			for _, c := range conns {
				if c != nil {
					c.Close()
				}
			}
		}
	}
}
*/

func (c *Connection) Send(data []byte) error {

	buff := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(buff[0:4], uint32(len(data)))
	copy(buff[4:], data)

	go c.Recv()
	_, err := c.Conn.Write(buff)

	if err != nil {
		logger.Error("write err:", err)
		return err
	}

	//	fmt.Println("tcp write done")
	c.HasRequest <- true
	return nil
}

func (c *Connection) Recv() {
	for {
		select {
		case <-c.HasRequest:
			buf := make([]byte, 4)
			var size int32
			var data []byte
			reader := bufio.NewReader(c.Conn)
			// 前四个字节一般表示网络包大小
			if count, err := io.ReadFull(reader, buf); err == nil && count == 4 {

				sbuf := bytes.NewBuffer(buf)
				// 先读数据长度
				binary.Read(sbuf, binary.BigEndian, &size)

				// 固定解析出的数据的存储空间
				data = make([]byte, size)

				// 整段读取
				count, err := io.ReadFull(reader, data)

				if err != nil {
					if err == syscall.EPIPE {
						logger.Error("io read err:", err)
					}
				}
				// 确认数据长度和从tcp 协议中获取的长度是否相同
				if count != int(size) {
					logger.Error("wrong count")
				}
				c.Data <- data
			}

		case <-c.RecvDone:
			connections[c.Mytype].Conns[c.Addr] <- c
			return

		}
	}
}

func (c *Connection) getData(tag string) (interface{}, error) {
	//TODO 这里可以加 retry
	//timeout := time.After(1 * time.Second)
	timeout := time.After(1000 * time.Millisecond)
	//	tick := time.Tick(500 * time.Millisecond)

	for {
		select {
		/*
		   case <-tick:
		       rawdata := <-c.Data
		       fmt.Println(time.Now())
		*/
		case rawdata := <-c.Data:
			if rawdata != nil {
				newdata := c.ProtoUnserialize(rawdata, tag)
				return newdata, nil
			}
		case <-timeout:
			logger.Info("time out 1000 millisecond")
			nildata := c.ProtoUnserialize(nil, tag)
			c.ok = false
			//CloseConns(c)
			//TODO 超时关闭连接并重建连接？
			//可以将这个连接放到useless里，然后删除useless的连接并建立新连接放回列表

			//超时的话上一个包怎么丢弃？

			return nildata, errors.New("time out in 1 second")
		}
	}
}

func (c *Connection) ProtoUnserialize(data []byte, tag string) interface{} {
	if tag == "meta" {
		newdata := &ZPMeta.MetaCmdResponse{}
		// protobuf 解析
		err := proto.Unmarshal(data, newdata)
		if err != nil {
			logger.Error("unmarshaling error: ", err, c.Addr)
		}
		//    logger.Info(newdata)
		return newdata
	} else if tag == "node" {
		newdata := &client.CmdResponse{}
		// protobuf 解析
		err := proto.Unmarshal(data, newdata)
		if err != nil {
			logger.Error("unmarshaling error: ", err, c.Addr)
		}
		//logger.Info(newdata)
		return newdata
	}

	return nil
}

func GetMetaConn() *Connection {
	connections["meta"].lock.RLock()
	defer connections["meta"].lock.RLock()
	addrnum := rand.Intn(len(connections["meta"].Addrs))
	addr := connections["meta"].Addrs[addrnum]
	//conn := <-connections["meta"].Conns[addr]
	for {
		select {
		case conn := <-connections["meta"].Conns[addr]:
			if time.Since(conn.timestamp).Seconds() > float64(50*time.Second) {
				uselessConns <- conn
			} else {
				return conn
			}
		}
	}
	//connnum := rand.Intn(len(connections["meta"].Conns[addr]))
	/*
		var conn *Connection
		for conn := range connections["meta"].Conns[addr] {
			if conn.ok == true {
				return conn
			} else {
				uselessConns <- conn
			}
		}
		//logger.Info("meta conn num: ", connnum)
	*/

}

func GetNodeConn(addr string) *Connection {
	//connections["node"].lock.RLock()
	//defer connections["node"].lock.RLock()
	//connnum := rand.Intn(len(connections["node"].Conns[addr]))
	//conn := <-connections["node"].Conns[addr]
	for {
		select {
		case conn := <-connections["node"].Conns[addr]:
			if time.Since(conn.timestamp).Seconds() > float64(50*time.Second) {
				uselessConns <- conn
			} else {
				return conn
			}
		}
	}
	/*
		var conn *Connection

		for {
			select {
			case conn.ok == true:

			}
		}
		for conn := range connections["node"].Conns[addr] {
			if conn.ok == true {
				return conn
			} else {
				uselessConns <- conn
			}
		}
	*/
	//logger.Info("node conn num: ", connnum)

	//return conn
}
