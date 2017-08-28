package zeppelin

import (
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/tinytub/zep-exporter/proto/ZPMeta"
	"github.com/tinytub/zep-exporter/proto/client"

	"github.com/golang/protobuf/proto"
)

var (
	namespace = "zeppelin"
	//subsystem = "zepCmd"
	cmdError = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		//Subsystem: subsystem,
		Name: "CmdErr",
		Help: "zep exporter command error",
	},
		[]string{"cmd", "code"},
	)
)

func init() {
	prometheus.MustRegister(cmdError)
}

//TODO: node 节点由外部选择
func (c *Connection) PullTable(tablename string) (*ZPMeta.MetaCmdResponse, error) {
	cmd, err := c.MakeCmdPull(tablename, "", 0)
	if err != nil {
		logger.Info("marshal proto error", err)
		//fmt.Println("marshal proto error", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	c.Send(cmd)
	data, err := c.getData("meta")
	if data.(*ZPMeta.MetaCmdResponse).GetCode() != 0 {
		err = errors.New(data.(*ZPMeta.MetaCmdResponse).GetMsg())
		cmdError.WithLabelValues("PullTable", string(data.(*ZPMeta.MetaCmdResponse).GetCode())).Inc()
	}
	if err != nil {
		cmdError.WithLabelValues("PullTable", "999").Inc()
		return data.(*ZPMeta.MetaCmdResponse), err
	}
	return data.(*ZPMeta.MetaCmdResponse), nil
}

func (c *Connection) MakeCmdPull(name string, node string, port int32) ([]byte, error) {
	var raw_cmd *ZPMeta.MetaCmd
	switch {
	case name != "":
		raw_cmd = &ZPMeta.MetaCmd{
			Type: ZPMeta.Type_PULL.Enum(),
			Pull: &ZPMeta.MetaCmd_Pull{Name: &name},
		}
	case node != "" && port != 0:

		raw_cmd = &ZPMeta.MetaCmd{
			Type: ZPMeta.Type_PULL.Enum(),
			Pull: &ZPMeta.MetaCmd_Pull{Node: &ZPMeta.Node{Ip: &node,
				Port: &port},
			},
		}
	}

	return proto.Marshal(raw_cmd)
}

func (c *Connection) PullNode(node string, port int32) (*ZPMeta.MetaCmdResponse, error) {
	//cmd, err := c.MakeCmdPullnode(node, port)
	cmd, err := c.MakeCmdPull("", node, port)
	if err != nil {
		logger.Info("marshal proto error", err)
		//fmt.Println("marshal proto error", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.Send(cmd)
	data, err := c.getData("meta")
	if data.(*ZPMeta.MetaCmdResponse).GetCode() != 0 {
		err = errors.New(data.(*ZPMeta.MetaCmdResponse).GetMsg())
		cmdError.WithLabelValues("PullNode", string(data.(*ZPMeta.MetaCmdResponse).GetCode())).Inc()
	}
	if err != nil {
		cmdError.WithLabelValues("PullNode", "999").Inc()
		return data.(*ZPMeta.MetaCmdResponse), err
	}

	return data.(*ZPMeta.MetaCmdResponse), nil
}

func (c *Connection) ListTable() (*ZPMeta.MetaCmdResponse, error) {

	cmd, err := c.MakeCmdListTable()
	if err != nil {
		logger.Info("marshal proto error", err)
		//fmt.Println("marshal proto error", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.Send(cmd)
	data, err := c.getData("meta")
	if data.(*ZPMeta.MetaCmdResponse).GetCode() != 0 {
		err = errors.New(data.(*ZPMeta.MetaCmdResponse).GetMsg())
		cmdError.WithLabelValues("ListTable", string(data.(*ZPMeta.MetaCmdResponse).GetCode())).Inc()
	}
	if err != nil {
		cmdError.WithLabelValues("ListTable", "999").Inc()
		return data.(*ZPMeta.MetaCmdResponse), err
	}

	return data.(*ZPMeta.MetaCmdResponse), nil

}

func (c *Connection) MakeCmdListTable() ([]byte, error) {

	raw_cmd := &ZPMeta.MetaCmd{
		Type: ZPMeta.Type_LISTTABLE.Enum(),
	}

	return proto.Marshal(raw_cmd)
}

func (c *Connection) ListNode() (*ZPMeta.MetaCmdResponse, error) {
	cmd, err := c.MakeCmdListNode()
	if err != nil {
		logger.Info("marshal proto error", err)
		//fmt.Println("marshal proto error", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	c.Send(cmd)
	data, err := c.getData("meta")
	if data.(*ZPMeta.MetaCmdResponse).GetCode() != 0 {
		cmdError.WithLabelValues("ListNode", string(data.(*ZPMeta.MetaCmdResponse).GetCode())).Inc()
		err = errors.New(data.(*ZPMeta.MetaCmdResponse).GetMsg())
	}
	if err != nil {
		cmdError.WithLabelValues("ListNode", "999").Inc()
		return data.(*ZPMeta.MetaCmdResponse), err
	}

	return data.(*ZPMeta.MetaCmdResponse), nil
}

func (c *Connection) MakeCmdListNode() ([]byte, error) {

	raw_cmd := &ZPMeta.MetaCmd{
		Type: ZPMeta.Type_LISTNODE.Enum(),
	}

	return proto.Marshal(raw_cmd)
}

func (c *Connection) ListMeta() (*ZPMeta.MetaCmdResponse, error) {
	cmd, err := c.MakeCmdListMeta()
	if err != nil {
		logger.Info("marshal proto error", err)
		//fmt.Println("marshal proto error", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.Send(cmd)
	data, err := c.getData("meta")
	if data.(*ZPMeta.MetaCmdResponse).GetCode() != 0 {
		cmdError.WithLabelValues("ListMeta", string(data.(*ZPMeta.MetaCmdResponse).GetCode())).Inc()
		err = errors.New(data.(*ZPMeta.MetaCmdResponse).GetMsg())
	}
	if err != nil {
		cmdError.WithLabelValues("ListMeta", "999").Inc()
		return data.(*ZPMeta.MetaCmdResponse), err
	}

	return data.(*ZPMeta.MetaCmdResponse), nil
}

func (c *Connection) MakeCmdListMeta() ([]byte, error) {

	raw_cmd := &ZPMeta.MetaCmd{
		Type: ZPMeta.Type_LISTMETA.Enum(),
	}

	return proto.Marshal(raw_cmd)
}

func (c *Connection) CreateTable(name string, num int32) (*ZPMeta.MetaCmdResponse, error) {
	cmd, err := c.MakeCmdCreateTable(name, num)
	if err != nil {
		logger.Info("marshal proto error", err)
		//fmt.Println("marshal proto error", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.Send(cmd)
	data, err := c.getData("meta")
	if data.(*ZPMeta.MetaCmdResponse).GetCode() != 0 {
		cmdError.WithLabelValues("CreateTable", string(data.(*ZPMeta.MetaCmdResponse).GetCode())).Inc()
		err = errors.New(data.(*ZPMeta.MetaCmdResponse).GetMsg())
	}
	if err != nil {
		cmdError.WithLabelValues("CreateTable", "999").Inc()
		return data.(*ZPMeta.MetaCmdResponse), err
	}

	return data.(*ZPMeta.MetaCmdResponse), nil
}

func (c *Connection) MakeCmdCreateTable(name string, num int32) ([]byte, error) {

	raw_cmd := &ZPMeta.MetaCmd{
		Type: ZPMeta.Type_INIT.Enum(),
		Init: &ZPMeta.MetaCmd_Init{
			Name: &name,
			Num:  &num,
		},
	}
	return proto.Marshal(raw_cmd)
}

// client
func (c *Connection) InfoStats(tablename string) (*client.CmdResponse, error) {
	c.mu.Lock()
	cmd, err := c.MakeCmdInfoStats(tablename)
	if err != nil {
		logger.Info("marshal proto error", err)
		//fmt.Println("marshal proto error", err)
	}

	c.Send(cmd)
	defer c.mu.Unlock()

	data, err := c.getData("node")
	if data.(*client.CmdResponse).GetCode() != 0 {
		cmdError.WithLabelValues("InfoStats", string(data.(*client.CmdResponse).GetCode())).Inc()
		err = errors.New(data.(*client.CmdResponse).GetMsg())
	}
	if err != nil {
		cmdError.WithLabelValues("InfoStats", "999").Inc()
		return data.(*client.CmdResponse), err
	}

	return data.(*client.CmdResponse), nil
}

/*
func (c *Connection) TotalQPS(nodeconns map[string]*Connection, table string) int {
    var totalQPS int
    for _, nConn := range nodeconns {
        go nConn.Recv()
        data, err := nConn.InfoStats(table)
        if err == nil {
            infoStats := data.GetInfoStats()
            totalQPS = totalQPS + int(*infoStats[0].Qps)
        }
        nConn.RecvDone <- true
    }
    return totalQPS

}
*/

func (c *Connection) MakeCmdInfoStats(tablename string) ([]byte, error) {
	//logger.Info("tablename is:", tablename)
	raw_cmd := &client.CmdRequest{
		Type: client.Type_INFOSTATS.Enum(),
		Info: &client.CmdRequest_Info{TableName: &tablename},
	}

	return proto.Marshal(raw_cmd)
}

func (c *Connection) InfoCapacity(tablename string) (*client.CmdResponse, error) {
	cmd, err := c.MakeCmdInfoCapacity(tablename)
	if err != nil {
		logger.Info("marshal proto error", err)
		//fmt.Println("marshal proto error", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.Send(cmd)

	data, err := c.getData("node")
	if data.(*client.CmdResponse).GetCode() != 0 {
		cmdError.WithLabelValues("InfoCapacity", string(data.(*client.CmdResponse).GetCode())).Inc()
		err = errors.New(data.(*client.CmdResponse).GetMsg())
	}
	if err != nil {
		cmdError.WithLabelValues("InfoCapacity", "999").Inc()
		return data.(*client.CmdResponse), err
	}

	return data.(*client.CmdResponse), nil
}

func (c *Connection) MakeCmdInfoCapacity(tablename string) ([]byte, error) {
	//logger.Info("tablename is:", tablename)
	raw_cmd := &client.CmdRequest{
		Type: client.Type_INFOCAPACITY.Enum(),
		Info: &client.CmdRequest_Info{TableName: &tablename},
	}

	return proto.Marshal(raw_cmd)
}

func (c *Connection) InfoRepl(tablename string) (*client.CmdResponse, error) {
	cmd, err := c.MakeCmdInfoRepl(tablename)
	if err != nil {
		logger.Info("marshal proto error", err)
		//fmt.Println("marshal proto error", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.Send(cmd)
	data, err := c.getData("node")
	if data.(*client.CmdResponse).GetCode() != 0 {
		cmdError.WithLabelValues("InfoRepl", string(data.(*client.CmdResponse).GetCode())).Inc()
		err = errors.New(data.(*client.CmdResponse).GetMsg())
	}
	if err != nil {
		cmdError.WithLabelValues("InfoRepl", "999").Inc()
		return data.(*client.CmdResponse), err
	}

	return data.(*client.CmdResponse), nil
}

func (c *Connection) MakeCmdInfoRepl(tablename string) ([]byte, error) {
	//logger.Info("tablename is:", tablename)
	raw_cmd := &client.CmdRequest{
		Type: client.Type_INFOREPL.Enum(),
		Info: &client.CmdRequest_Info{TableName: &tablename},
	}

	return proto.Marshal(raw_cmd)
}

func (c *Connection) InfoServer() (*client.CmdResponse, error) {
	cmd, err := c.MakeCmdInfoServer()
	if err != nil {
		logger.Info("marshal proto error", err)
		//fmt.Println("marshal proto error", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.Send(cmd)
	data, err := c.getData("node")
	if data.(*client.CmdResponse).GetCode() != 0 {
		cmdError.WithLabelValues("InfoServer", string(data.(*client.CmdResponse).GetCode())).Inc()
		err = errors.New(data.(*client.CmdResponse).GetMsg())
	}
	if err != nil {
		cmdError.WithLabelValues("InfoServer", "999").Inc()
		return data.(*client.CmdResponse), err
	}

	return data.(*client.CmdResponse), nil
}

func (c *Connection) MakeCmdInfoServer() ([]byte, error) {
	//logger.Info("tablename is:", tablename)
	raw_cmd := &client.CmdRequest{
		Type: client.Type_INFOSERVER.Enum(),
		//需不需要传info？
		//Info: &client.CmdRequest_Info{TableName: &tablename},
	}

	return proto.Marshal(raw_cmd)
}

/*
func (c *Connection) Ping() bool {
    c.Send(&PingPacket{})

    select {
    case _, ok := <-c.pongs:
        return ok
    case <-time.After(500 * time.Millisecond):
        return false
    }
}
*/

func (c *Connection) Set(tablename string, key string, value []byte) (*client.CmdResponse, error) {
	cmd, err := c.MakeCmdSet(tablename, key, value)
	if err != nil {
		logger.Info("marshal proto error", err)
		//fmt.Println("marshal proto error", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	c.Send(cmd)
	data, err := c.getData("node")
	if data.(*client.CmdResponse).GetCode() != 0 {
		cmdError.WithLabelValues("Set", string(data.(*client.CmdResponse).GetCode())).Inc()
		err = errors.New(data.(*client.CmdResponse).GetMsg())
	}
	if err != nil {
		cmdError.WithLabelValues("Set", "999").Inc()
		return data.(*client.CmdResponse), err
	}

	return data.(*client.CmdResponse), nil
}

func (c *Connection) MakeCmdSet(tablename string, key string, value []byte) ([]byte, error) {
	raw_cmd := &client.CmdRequest{
		Type: client.Type_SET.Enum(),
		Set: &client.CmdRequest_Set{
			TableName: &tablename,
			Key:       &key,
			Value:     value,
		},
	}
	return proto.Marshal(raw_cmd)
}

func (c *Connection) Get(tablename string, key string) (*client.CmdResponse, error) {
	cmd, err := c.MakeCmdGet(tablename, key)
	if err != nil {
		logger.Info("marshal proto error", err)
		//fmt.Println("marshal proto error", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.Send(cmd)
	data, err := c.getData("node")
	if data.(*client.CmdResponse).GetCode() != 0 {
		cmdError.WithLabelValues("Get", fmt.Sprint(data.(*client.CmdResponse).GetCode())).Inc()
		err = errors.New(data.(*client.CmdResponse).GetMsg())
	}
	if err != nil {
		cmdError.WithLabelValues("Get", "999").Inc()
		return data.(*client.CmdResponse), err
	}

	return data.(*client.CmdResponse), nil
}

func (c *Connection) MakeCmdGet(tablename string, key string) ([]byte, error) {
	raw_cmd := &client.CmdRequest{
		Type: client.Type_GET.Enum(),
		Get: &client.CmdRequest_Get{
			TableName: &tablename,
			Key:       &key,
			//Value:     value,
		},
	}
	return proto.Marshal(raw_cmd)
}

func (c *Connection) Ping() (*ZPMeta.MetaCmdResponse, error) {
	cmd, err := c.MakeCmdPing()
	if err != nil {
		logger.Info("marshal proto error", err)
		//fmt.Println("marshal proto error", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.Send(cmd)
	data, err := c.getData("meta")
	if data.(*client.CmdResponse).GetCode() != 0 {
		cmdError.WithLabelValues("Ping", fmt.Sprint(data.(*client.CmdResponse).GetCode())).Inc()
		err = errors.New(data.(*client.CmdResponse).GetMsg())
	}
	if err != nil {
		cmdError.WithLabelValues("Ping", "999").Inc()
		return data.(*ZPMeta.MetaCmdResponse), err
	}

	return data.(*ZPMeta.MetaCmdResponse), nil
}

func (c *Connection) MakeCmdPing() ([]byte, error) {
	raw_cmd := &ZPMeta.MetaCmd{
		Type: ZPMeta.Type_PING.Enum(),
	}
	return proto.Marshal(raw_cmd)
}

//func (c *Connection) getData(tag string) *ZPMeta.MetaCmdResponse {
func (c *Connection) getData(tag string) (interface{}, error) {
	//TODO 这里可以加 retry
	timeout := time.After(1 * time.Second)
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
				//close(c.RecvDone)
				return newdata, nil
			}
		case <-timeout:
			logger.Info("time out 1 second")
			//fmt.Println("time out 1 second")
			nildata := c.ProtoUnserialize(nil, tag)
			//c.Conn.Close()
			return nildata, errors.New("time out in 1 second")
			/*
			   case <-time.After(5000 * time.Millisecond):
			       logger.Info("time out 5000ms")
			       //		return &ZPMeta.MetaCmdResponse{}
			       //return nil
			       continue
			*/
		}
	}
}

func (c *Connection) NilStruct(tag string) {

}
