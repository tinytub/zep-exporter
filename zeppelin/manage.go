package zeppelin

/*
#cgo CFLAGS: -I ${SRCDIR}/include
#cgo LDFLAGS: -L ${SRCDIR}/lib -lchash -lstdc++

#include "chash.h"
*/
import "C"
import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strconv"

	"github.com/tinytub/zep-exporter/proto/ZPMeta"
	"github.com/tinytub/zep-exporter/proto/client"
)

func ListNode() ([]*ZPMeta.NodeStatus, error) {
	conn := GetMetaConn()

	/*
		if err != nil {
			return nil, err
		}
	*/

	data, _ := conn.ListNode()
	conn.RecvDone <- true
	if data.Code.String() != "OK" {
		return nil, errors.New(*data.Msg)
	}
	nodes := data.ListNode.Nodes.Nodes

	return nodes, nil
}

type MetaNodes struct {
	Followers []*Node
	Leader    *Node
}
type Node struct {
	Ip   *string
	Port *int32
}

func ListMeta(addrs []string) (*ZPMeta.MetaNodes, error) {
	conn := GetMetaConn()

	/*
		if err != nil {
			return &ZPMeta.MetaNodes{}, err
		}
	*/
	data, _ := conn.ListMeta()
	if data.Code.String() != "OK" {
		return &ZPMeta.MetaNodes{}, errors.New(*data.Msg)
	}
	metas := data.ListMeta.Nodes

	conn.RecvDone <- true
	return metas, nil
}

func ListTable() (*ZPMeta.TableName, error) {
	conn := GetMetaConn()
	/*
		if err != nil {
			return &ZPMeta.TableName{}, err
		}
	*/

	data, _ := conn.ListTable()
	if data.Code.String() != "OK" {
		return &ZPMeta.TableName{}, errors.New(*data.Msg)
	}
	tables := data.ListTable.Tables
	conn.RecvDone <- true
	return tables, nil
}

type PTable struct {
	pull       *ZPMeta.Table
	TableEpoch int32
	nodelist   []*ZPMeta.NodeStatus
}

func PullTable(tablename string) (PTable, error) {
	var ptable PTable
	conn := GetMetaConn()

	pullResp, err := conn.PullTable(tablename)
	conn.RecvDone <- true
	if len(pullResp.Pull.GetInfo()) != 0 {
		ptable.pull = pullResp.Pull.GetInfo()[0]
		ptable.TableEpoch = pullResp.Pull.GetVersion()
	}
	ptable.nodelist, err = ListNode()

	if err != nil {
		return ptable, err
	}

	return ptable, nil
}

func CreateTable(name string, num int32, addrs []string) {
	conn := GetMetaConn()
	/*
		if err != nil {
			//return nil, err
		}
	*/

	data, _ := conn.CreateTable(name, num)
	if data.Code.String() != "OK" {
		logger.Warningf(*data.Msg)
		os.Exit(0)
	}
	//fmt.Println(data)
	conn.RecvDone <- true
	return
}

func Ping(addrs []string) {
	conn := GetMetaConn()
	/*
		if err != nil {
			//return nil, err
		}
	*/

	//conn.mu.Lock()
	data, _ := conn.Ping()
	//fmt.Println(data)
	if data.Code.String() != "OK" {
		logger.Warningf(*data.Msg)
		os.Exit(0)
	}
	fmt.Println(data)
	conn.RecvDone <- true
	return
}

func Set(tablename string, key string, value string, addrs []string) {
	partlocale := locationPartition(tablename, key, addrs)
	fmt.Println(partlocale)

	//TODO: 这里node相关的是怎么处理的来着?
	conn := GetNodeConn(partlocale[0])

	/*
		fmt.Println(Nconn)
		infostats, _ := Nconn.InfoStats(tablename)
		fmt.Println(infostats)
	*/
	v := []byte(value)
	setresp, err := conn.Set(tablename, key, v)
	if err != nil {
		fmt.Println(err)
	}
	//fmt.Println(setresp)
}

func Get(tablename string, key string, value string, addrs []string) {
	partlocale := locationPartition(tablename, key, addrs)
	conn := GetNodeConn(partlocale[0])
	//Nconn, err := NewConn(partlocale)

	getresp, err := conn.Get(tablename, key)
	conn.RecvDone <- true
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(getresp)

}

func (ptable *PTable) Space(tablename string) (int64, int64, error) {
	// pull table ---> get partition master info state ---> calculate
	/*
		Mconn, err := NewConn(addrs)
		if err != nil {
			return 0, 0, err
		}

		pullResp, err := Mconn.PullTable(tablename)
		if err != nil {
			return 0, 0, err
		}
		Mconn.RecvDone <- true
		pull := pullResp.Pull.GetInfo()[0]
	*/
	pull := ptable.pull
	//	var masters []string
	var used int64 = 0
	var remain int64 = 0

	worker := runtime.NumCPU()
	//fmt.Println(worker)
	jobs := make(chan Jobber, worker)
	results := make(chan Result, len(pull.GetPartitions()))
	dones := make(chan struct{}, worker)

	//go addJob(pull.GetPartitions(), jobs, results, &JobInfoCap{})
	//通过 addjob 不太好搞...只能闭包构造 jobs 管道
	go func() {
		//var pmaddrs map[string]string
		pmaddrs := make(map[string]int)

		for _, partition := range pull.GetPartitions() {
			ip := partition.Master.GetIp()
			port := strconv.Itoa(int(partition.Master.GetPort()))
			pmaddrs[ip+":"+port] = 0
			//jobs <- job{addr, results}
		}
		for addr, _ := range pmaddrs {
			jobs <- &JobInfoCap{addr, results}
		}
		close(jobs)
	}()

	for i := 0; i < worker; i++ {
		go doJob(jobs, dones, tablename)
	}
	data := awaitForCloseResult(dones, results, worker)
	for _, d := range data {
		used += d.Used
		remain += d.Remain
	}
	return used, remain, nil

}

func (ptable *PTable) Stats(tablename string) (int64, int32, error) {
	/*
		Mconn, err := NewConn(addrs)
		if err != nil {
			return 0, 0, err
		}

		pullResp, err := Mconn.PullTable(tablename)
		if err != nil {
			return 0, 0, err
		}
		Mconn.RecvDone <- true
		pull := pullResp.Pull.GetInfo()[0]
	*/
	pull := ptable.pull

	//	var masters []string

	var query int64
	var qps int32

	worker := runtime.NumCPU()
	//fmt.Println(worker)
	jobs := make(chan Jobber, worker)
	results := make(chan Result, len(pull.GetPartitions()))
	dones := make(chan struct{}, worker)

	go func() {
		pmaddrs := make(map[string]int)
		for _, partition := range pull.GetPartitions() {
			ip := partition.Master.GetIp()
			port := strconv.Itoa(int(partition.Master.GetPort()))
			pmaddrs[ip+":"+port] = 0

			//pmaddrs[ip] = port
			//jobs <- job{addr, results}
		}
		for addr, _ := range pmaddrs {
			jobs <- &JobInfoStats{addr, results}
		}
		close(jobs)
	}()

	for i := 0; i < worker; i++ {
		go doJob(jobs, dones, tablename)
	}
	data := awaitForCloseResult(dones, results, worker)
	for _, d := range data {
		query += d.Query
		qps += d.QPS
	}
	return query, qps, nil
}

type Unsyncoffset struct {
	Addr   string
	Offset float64
	//partition int32
}

func (ptable *PTable) Offset(tablename string) (map[int32][]*Unsyncoffset, error) {
	unsyncoffset := make(map[int32][]*Unsyncoffset)

	//var unsyncoffset Unsyncoffset
	/*

		Mconn, err := NewConn(addrs)
		if err != nil {
			return unsyncoffset, err
		}

		pullResp, err := Mconn.PullTable(tablename)
		if err != nil {
			return unsyncoffset, err
		}
		Mconn.RecvDone <- true
		pull := pullResp.Pull.GetInfo()[0]
	*/
	pull := ptable.pull

	// 获取 各 master 的 offset, 再获取各 slave 的 offset,然后对 offset 进行比对

	//	var masters []string

	worker := runtime.NumCPU()
	//fmt.Println(worker)
	jobsMaster := make(chan Jobber, worker)
	resultsMaster := make(chan Result, len(pull.GetPartitions()))
	donesMaster := make(chan struct{}, worker)

	//add job
	go func() {
		pmaddrs := make(map[string]int)
		for _, partition := range pull.GetPartitions() {
			mip := partition.Master.GetIp()
			mport := strconv.Itoa(int(partition.Master.GetPort()))
			//ip + port 这种 map 方式不合理
			pmaddrs[mip+":"+mport] = 0
			/*
				for _, slave := range partition.GetSlaves() {

					sip := slave.GetIp()
					sport := strconv.Itoa(int(slave.GetPort()))
					for _, node := range ptable.nodelist {
						if node.GetStatus() == 0 && node.Node.GetIp() == sip && node.Node.GetPort() == slave.GetPort() {
							pmaddrs[sip+":"+sport] = 0

						}
					}
					//jobs <- job{addr, results}
				}
			*/
			//jobs <- job{addr, results}
		}
		for addr, _ := range pmaddrs {
			jobsMaster <- &JobOffset{addr, resultsMaster}
		}
		close(jobsMaster)
	}()

	//do master job
	for i := 0; i < worker; i++ {
		go doJob(jobsMaster, donesMaster, tablename)
	}

	dataMaster := awaitForCloseResult(donesMaster, resultsMaster, worker)

	//		alloffsets := make(map[string][]*client.SyncOffset)
	alloffsets := make(map[string]map[int32]*client.SyncOffset)

	for _, d := range dataMaster {
		singleoffsets := make(map[int32]*client.SyncOffset)
		for _, state := range d.Pstate {
			singleoffsets[state.GetPartitionId()] = state.GetSyncOffset()
		}
		alloffsets[d.addr] = singleoffsets
	}

	for _, partition := range pull.GetPartitions() {
		mIp := partition.Master.GetIp()
		mPort := strconv.Itoa(int(partition.Master.GetPort()))
		maddr := mIp + ":" + mPort

		if v, ok := alloffsets[maddr]; ok && len(partition.Slaves) > 0 {
			masteroffset := v[partition.GetId()]
			//unsyncoffset[partition.GetId()] = make([]*Unsyncoffset, len(partition.GetSlaves()))
			for _, slave := range partition.GetSlaves() {

				for _, node := range ptable.nodelist {
					if node.GetStatus() == 0 && node.Node.GetIp() == slave.GetIp() && node.Node.GetPort() == slave.GetPort() {
						//logger.Info("slave ip: ", slave.GetIp())
						sIp := slave.GetIp()
						// out of range ?
						//logger.Info("slave port: ", slave.GetPort())
						sPort := strconv.Itoa(int(slave.GetPort()))
						saddr := sIp + ":" + sPort
						//logger.Info("slave addr: ", saddr)
						slaveParts := alloffsets[saddr]
						if len(slaveParts) == 0 {
							continue
						}
						slaveoffset := slaveParts[partition.GetId()]
						if !reflect.DeepEqual(masteroffset, slaveoffset) {
							offset := (float64(masteroffset.GetFilenum()) - float64(slaveoffset.GetFilenum()))
							//fmt.Println("mfile: ", masteroffset.GetFilenum())
							//fmt.Println("afile: ", slaveoffset.GetFilenum())

							if offset == 0 {
								offset = float64((masteroffset.GetOffset() - slaveoffset.GetOffset())) / 1000000000
								//fmt.Println("moff: ", masteroffset.GetOffset())
								//fmt.Println("soff: ", slaveoffset.GetOffset())
							}
							//logger.Infof("tablename:%s, masterFileNum: %d, slaveFileNum: %d, masterOffset: %d, slaveOffset: %d, offsetDiff: %f\n", tablename, masteroffset.GetFilenum(), slaveoffset.GetFilenum(), masteroffset.GetOffset(), slaveoffset.GetOffset(), offset)
							unsyncoffset[partition.GetId()] = append(unsyncoffset[partition.GetId()], &Unsyncoffset{Addr: saddr, Offset: offset})
						} else {
							unsyncoffset[partition.GetId()] = append(unsyncoffset[partition.GetId()], &Unsyncoffset{Addr: saddr, Offset: 0})

						}

					}
				}

			}
		}

	}
	//fmt.Println(unsyncoffset)
	//fmt.Println(dataSlave)
	return unsyncoffset, nil
}

type nodeEpoch struct {
	Addr  string
	Epoch int64
}

func (ptable *PTable) Server() ([]*nodeEpoch, error) {
	pull := ptable.pull

	//	var masters []string

	worker := runtime.NumCPU()
	//fmt.Println(worker)
	jobs := make(chan WithNothingJobber, worker)
	results := make(chan Result, len(pull.GetPartitions()))
	dones := make(chan struct{}, worker)

	go func() {
		pmaddrs := make(map[string]int)
		for _, partition := range pull.GetPartitions() {
			ip := partition.Master.GetIp()
			port := strconv.Itoa(int(partition.Master.GetPort()))
			pmaddrs[ip+":"+port] = 0

			//pmaddrs[ip] = port
			//jobs <- job{addr, results}
		}
		for addr, _ := range pmaddrs {
			jobs <- &JobInfoServer{addr, results}
		}
		close(jobs)
	}()

	for i := 0; i < worker; i++ {
		go doWithNothingJob(jobs, dones)
	}
	data := awaitForCloseResult(dones, results, worker)
	nEpoch := make([]*nodeEpoch, 0, len(data))
	for _, d := range data {
		nEpoch = append(nEpoch, &nodeEpoch{d.addr, d.NodeEpoch})
	}
	return nEpoch, nil
}

func doJob(jobs <-chan Jobber, dones chan<- struct{}, tablename string) {
	for job := range jobs {
		job.Do(tablename)
	}
	dones <- struct{}{}
}
func doWithNothingJob(jobs <-chan WithNothingJobber, dones chan<- struct{}) {
	for job := range jobs {
		job.Do()
	}
	dones <- struct{}{}
}

type Jobber interface {
	Do(tablename string)
}
type WithNothingJobber interface {
	Do()
}

type JobInfoCap struct {
	addr   string
	result chan<- Result
}

type JobInfoStats struct {
	addr   string
	result chan<- Result
}

type JobOffset struct {
	addr   string
	result chan<- Result
}
type JobInfoServer struct {
	addr   string
	result chan<- Result
}

type Result struct {
	addr      string
	Used      int64
	Remain    int64
	QPS       int32
	Query     int64
	Pstate    []*client.PartitionState
	Offsets   []*client.SyncOffset
	ErrMsg    string
	NodeEpoch int64
}

func (job *JobInfoCap) Do(tablename string) {
	//addr := partition.Master.GetIp() + ":" + strconv.Itoa(int(partition.Master.GetPort()))
	var used int64
	var remain int64
	//Nconn, errN := NewConn([]string{job.addr})
	conn := GetNodeConn(job.addr)
	/*
		if errN != nil {
			job.result <- Result{}
			return
		}
	*/

	inforesp, err := conn.InfoCapacity(tablename)
	conn.RecvDone <- true
	if err != nil {
		job.result <- Result{}
		return
	}
	infoCap := inforesp.GetInfoCapacity()
	for _, i := range infoCap {
		used += i.GetUsed()
		remain += i.GetRemain()
	}
	job.result <- Result{Used: used, Remain: remain}
}

func (job *JobInfoStats) Do(tablename string) {
	//addr := partition.Master.GetIp() + ":" + strconv.Itoa(int(partition.Master.GetPort()))
	var query int64
	var qps int32
	//Nconn, errN := NewConn([]string{job.addr})
	conn := GetNodeConn(job.addr)
	/*
		if errN != nil {
			job.result <- Result{}
			return
		}
	*/

	inforesp, err := conn.InfoStats(tablename)
	conn.RecvDone <- true
	if err != nil {
		job.result <- Result{}
		return
	}
	infoStats := inforesp.GetInfoStats()
	for _, i := range infoStats {
		//logger.Info(":", i, job.addr)
		query += i.GetTotalQuerys()
		qps += i.GetQps()
	}
	job.result <- Result{Query: query, QPS: qps}
}

func (job *JobOffset) Do(tablename string) {
	//addr := partition.Master.GetIp() + ":" + strconv.Itoa(int(partition.Master.GetPort()))

	//Nconn, errN := NewConn([]string{job.addr})
	conn := GetNodeConn(job.addr)
	/*
		if errN != nil {
			job.result <- Result{}
			return
		}
	*/
	inforesp, err := conn.InfoRepl(tablename)
	conn.RecvDone <- true
	if err != nil {
		job.result <- Result{}
		return
	}
	infoPart := inforesp.GetInfoRepl()
	var result Result
	//	result.Offsets = make(map[string][]*client.SyncOffset)
	for _, i := range infoPart {
		//fmt.Println("offset:", i.GetSyncOffset())
		result.Pstate = i.GetPartitionState()
		for _, p := range result.Pstate {
			//fmt.Println("offset:", p.GetSyncOffset(), p.GetPartitionId())
			result.Offsets = append(result.Offsets, p.GetSyncOffset())
		}
		result.addr = job.addr
	}
	job.result <- result
}

func (job *JobInfoServer) Do() {
	//addr := partition.Master.GetIp() + ":" + strconv.Itoa(int(partition.Master.GetPort()))
	//Nconn, errN := NewConn([]string{job.addr})
	conn := GetNodeConn(job.addr)

	/*
		if errN != nil {
			job.result <- Result{}
			return
		}
	*/

	inforesp, err := conn.InfoServer()
	conn.RecvDone <- true
	if err != nil {
		job.result <- Result{}
		return
	}

	infoServer := inforesp.GetInfoServer()
	epoch := infoServer.GetEpoch()
	job.result <- Result{NodeEpoch: epoch, addr: job.addr}

}

func awaitForCloseResult(dones <-chan struct{}, results chan Result, worker int) []Result {
	working := worker
	done := false
	//var totalused int64 = 0
	//var totalremain int64 = 0
	var data []Result
	for {
		select {
		case result := <-results:
			data = append(data, result)
			//totalused += result.Used
			//totalremain += result.Remain
		case <-dones:
			working -= 1
			if working <= 0 {
				done = true
			}
		default:
			if done {
				//fmt.Println("goroutine totalused", totalused)
				//fmt.Println("goroutine totalremain", totalremain)
				return data
			}
		}
	}
}

//
func locationPartition(tablename string, key string, addrs []string) []string {
	//Mconn, err := NewConn(addrs)
	conn := GetMetaConn()
	/*
		if err != nil {
			//return nil, err
		}
	*/

	tableinfo, _ := conn.PullTable(tablename)
	//./src/zp_table.cc:  int par_num = std::hash<std::string>()(key) % partitions_.size();

	/* 动态链接库的编译方法
	gcc -c chash.cc -std=c++11
	ar rv libchash.a chash.o
	mv libchash.a ../lib
	测试
	g++ -o chash chash.cc -std=c++11
	*/

	partcount := len(tableinfo.Pull.Info[0].Partitions)
	parNum := uint(C.chash(C.CString(key))) % uint(partcount)
	nodemaster := tableinfo.Pull.Info[0].Partitions[parNum].GetMaster()

	conn.RecvDone <- true
	var nodeaddrs []string
	nodeaddrs = append(nodeaddrs, nodemaster.GetIp()+":"+strconv.Itoa(int(nodemaster.GetPort())))
	return nodeaddrs

}

func getPartition(conn *Connection, tablename string) int {

	data, _ := conn.PullTable(tablename)

	if data.Code.String() != "OK" {
		logger.Warningf(*data.Msg)
		os.Exit(0)
	}

	var pNum int
	for _, part := range data.Pull.Info {
		pNum += len(part.Partitions)
	}
	//fmt.Println(pNum)
	return pNum
}

//func getBytes(key interface{}) ([]byte, error) {
func getBytes(key string) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(key)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
