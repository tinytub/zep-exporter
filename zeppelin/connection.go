package zeppelin

import (
	"bufio"
	"bytes"
	"encoding/binary"
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
	RefreshConnInterval   = 3 * time.Second
	UselessConnChanLen    = 200
	UselessConnRemoveWait = 180 * time.Second
	maxConnNum            = 3
)

type Connection struct {
	Conn       net.Conn
	Addr       string
	Data       chan []byte
	ok         int
	HasRequest chan bool
	RecvDone   chan bool
	mu         sync.Mutex
}

type TcpConns struct {
	Addrs []string
	Conns map[string][]*Connection
	lock  sync.RWMutex
}

var (
	connections  map[string]*TcpConns
	uselessConns chan []*Connection
)

//TODO:连接刷新和保活

func InitMetaConns(addrs []string) {
	connections = make(map[string]*TcpConns)
	connections["meta"] = &TcpConns{Addrs: []string{}, Conns: make(map[string][]*Connection)}
	// 地址从config里读
	//connections["meta"].Addrs = []string{"bada9305.add.bjyt.qihoo.net:9221"}
	connections["meta"].Addrs = addrs

	for _, addr := range connections["meta"].Addrs {
		for i := 1; i < maxConnNum; i++ {
			c := &Connection{}
			err := c.newConn(addr)
			if err != nil {
				logger.Error("bad conn, continue")
				continue
			}

			connections["meta"].Conns[addr] = append(connections["meta"].Conns[addr], c)
			go c.Recv()
		}
	}
	logger.Info("inited meta conns")
}

func InitNodeConns() {
	nodes, err := ListNode()
	if err != nil {
		logger.Error("can not get node list, initNodeConn err", err)
		os.Exit(1)
	}
	connections["node"] = &TcpConns{Addrs: []string{}, Conns: make(map[string][]*Connection)}

	for _, node := range nodes {
		if *node.Status != int32(1) {
			for i := 1; i < maxConnNum; i++ {
				nodeinfo := node.GetNode()
				addr := nodeinfo.GetIp() + ":" + strconv.Itoa(int(nodeinfo.GetPort()))
				c := &Connection{}
				err := c.newConn(addr)
				if err != nil {
					logger.Warning("bad conn, continue")
					continue
				}
				connections["node"].Conns[addr] = append(connections["node"].Conns[addr], c)
				go c.Recv()
				logger.Info("inited node conns", addr)
			}
		}
	}
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
		//logger.Info("recv", i)
		select {
		case <-c.HasRequest:
			buf := make([]byte, 4)
			var size int32
			var data []byte
			reader := bufio.NewReader(c.Conn)
			//logger.Info(c.Conn, i)
			// 前四个字节一般表示网络包大小
			if count, err := io.ReadFull(reader, buf); err == nil && count == 4 {

				//logger.Info("reading", i)
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
						//c.Conn.Close()
					}
					//			return
				}
				// 确认数据长度和从tcp 协议中获取的长度是否相同
				if count != int(size) {
					logger.Error("wrong count")
				}
				c.Data <- data
			}
		case <-c.RecvDone:
			continue

		case <-time.After(2000 * time.Millisecond):
			logger.Warning("recieve time out")
			continue
			//c.Conn.Close()
			//		return
			// 这里还应该在 case 一个 停止的 sigal, 或者看要不要设置超时.

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
	addr := connections["meta"].Addrs[rand.Intn(len(connections["meta"].Addrs))]
	conn := connections["meta"].Conns[addr][rand.Intn(len(connections["meta"].Conns[addr]))]
	return conn
}

func GetNodeConn(addr string) *Connection {
	connections["node"].lock.RLock()
	defer connections["node"].lock.RLock()
	conn := connections["node"].Conns[addr][rand.Intn(len(connections["node"].Conns[addr]))]
	return conn
}
