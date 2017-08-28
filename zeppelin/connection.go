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

	"github.com/tinytub/zep-exporter/proto/ZPMeta"
	"github.com/tinytub/zep-exporter/proto/client"

	"github.com/golang/protobuf/proto"
)

const maxConnNum = 3

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

var connections map[string]*TcpConns

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
				fmt.Println("bad conn, continue")
				continue
			}

			connections["meta"].Conns[addr] = append(connections["meta"].Conns[addr], c)
			go c.Recv()
		}
	}
	fmt.Println("inited meta conns")
}

func InitNodeConns() {
	nodes, err := ListNode()
	if err != nil {
		fmt.Println("can not get node list, initNodeConn err", err)
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
					fmt.Println("bad conn, continue")
					continue
				}
				connections["node"].Conns[addr] = append(connections["node"].Conns[addr], c)
				go c.Recv()
				fmt.Println("inited node conns", addr)
			}
		}
	}
	fmt.Println("inited all node conns")

}

func (c *Connection) newConn(addr string) error {
	conn, err := net.DialTimeout("tcp", addr, 1000*time.Millisecond)
	if err != nil {
		fmt.Println("tcp conn err:", err)
		return err
	}

	c.Data = make(chan []byte, 4)
	c.RecvDone = make(chan bool)
	c.HasRequest = make(chan bool)
	c.Conn = conn
	c.Addr = addr
	return nil
}

func (c *Connection) Send(data []byte) error {

	buff := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(buff[0:4], uint32(len(data)))

	copy(buff[4:], data)

	_, err := c.Conn.Write(buff)

	if err != nil {
		fmt.Println("write err:", err)
		return err
	}

	//	fmt.Println("tcp write done")
	c.HasRequest <- true
	return nil
}

func (c *Connection) Recv() {
	i := 1
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
						fmt.Println("io read err:", err)
						//c.Conn.Close()
					}
					//			return
				}
				// 确认数据长度和从tcp 协议中获取的长度是否相同
				if count != int(size) {
					fmt.Println("wrong count")
				}
				c.Data <- data
			}
		case <-c.RecvDone:
			//c.Conn.Close()
			//		return
		case <-time.After(2000 * time.Millisecond):
			//c.Conn.Close()
			//		return
			// 这里还应该在 case 一个 停止的 sigal, 或者看要不要设置超时.

		}
		i = i + 1
	}
}

func (c *Connection) ProtoUnserialize(data []byte, tag string) interface{} {
	if tag == "meta" {
		newdata := &ZPMeta.MetaCmdResponse{}
		// protobuf 解析
		err := proto.Unmarshal(data, newdata)
		if err != nil {
			fmt.Println("unmarshaling error: ", err, c.Addr)
		}
		//    logger.Info(newdata)
		return newdata
	} else if tag == "node" {
		newdata := &client.CmdResponse{}
		// protobuf 解析
		err := proto.Unmarshal(data, newdata)
		if err != nil {
			fmt.Println("unmarshaling error: ", err, c.Addr)
		}
		//logger.Info(newdata)
		return newdata
	}
	return nil
}

func GetMetaConn() *Connection {
	addr := connections["meta"].Addrs[rand.Intn(len(connections["meta"].Addrs))]
	conn := connections["meta"].Conns[addr][rand.Intn(len(connections["meta"].Conns[addr]))]
	return conn
}

func GetNodeConn(addr string) *Connection {
	conn := connections["node"].Conns[addr][rand.Intn(len(connections["node"].Conns[addr]))]
	return conn
}
