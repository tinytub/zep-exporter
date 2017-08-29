package zeppelin

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
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
	RefreshConnInterval   = 10 * time.Second
	UselessConnChanLen    = 200
	UselessConnRemoveWait = 60 * time.Second
	maxConnNum            = 3
)

type Connection struct {
	Conn net.Conn
	Addr string
	Data chan []byte
	//	Mynum      int
	Mytype     string
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
	uselessConns chan *Connection
)

//TODO:连接刷新和保活

func InitMetaConns(addrs []string) {
	connections = make(map[string]*TcpConns)
	connections["meta"] = &TcpConns{Addrs: []string{}, Conns: make(map[string][]*Connection)}
	// 地址从config里读
	//connections["meta"].Addrs = []string{"bada9305.add.bjyt.qihoo.net:9221"}
	connections["meta"].Addrs = addrs
	var wg sync.WaitGroup
	for _, addr := range connections["meta"].Addrs {
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

				connections["meta"].lock.Lock()
				defer connections["meta"].lock.Unlock()
				connections["meta"].Conns[addr] = append(connections["meta"].Conns[addr], c)
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
	connections["node"] = &TcpConns{Addrs: []string{}, Conns: make(map[string][]*Connection)}

	var wg sync.WaitGroup
	for _, node := range nodes {
		if *node.Status != int32(1) {
			for i := 0; i < maxConnNum; i++ {
				go func(node *ZPMeta.NodeStatus) {
					wg.Add(1)
					nodeinfo := node.GetNode()
					addr := nodeinfo.GetIp() + ":" + strconv.Itoa(int(nodeinfo.GetPort()))
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

					connections["node"].lock.Lock()
					defer connections["node"].lock.Unlock()
					connections["node"].Conns[addr] = append(connections["node"].Conns[addr], c)
					//go c.Recv()
					logger.Info("inited node conns", addr)
					wg.Done()
				}(node)
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

func refreshConns() {
	for cType, tconns := range connections {
		for addr, conns := range tconns.Conns {
			logger.Info(addr, len(conns))
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
					conns = append(conns, c)
					connections[cType].Conns[addr] = conns
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
	go c.Recv()
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
			return

			/*
				case <-time.After(2000 * time.Millisecond):
					logger.Warning("recieve time out")
					continue
			*/
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
	addrnum := rand.Intn(len(connections["meta"].Addrs))
	addr := connections["meta"].Addrs[addrnum]
	fmt.Println(len(connections["meta"].Conns[addr]))
	connnum := rand.Intn(len(connections["meta"].Conns[addr]))
	conn := connections["meta"].Conns[addr][connnum]
	logger.Info("meta conn num: ", connnum)

	return conn
}

func GetNodeConn(addr string) *Connection {
	connections["node"].lock.RLock()
	defer connections["node"].lock.RLock()
	connnum := rand.Intn(len(connections["node"].Conns[addr]))
	conn := connections["node"].Conns[addr][connnum]
	//logger.Info("node conn num: ", connnum)

	return conn
}
