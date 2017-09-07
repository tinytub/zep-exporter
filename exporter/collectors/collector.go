//   Copyright 2016 DigitalOcean
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package collectors

import (
	"strconv"
	"sync"

	logging "github.com/op/go-logging"
	//"github.com/prometheus/common/log"
	"github.com/tinytub/zep-exporter/zeppelin"

	"github.com/prometheus/client_golang/prometheus"
)

var logger = logging.MustGetLogger("collector")

const (
	namespace = "zeppelin"
)

// A ClusterUsageCollector is used to gather all the global stats about a given
// ceph cluster. It is sometimes essential to know how fast the cluster is growing
// or shrinking as a whole in order to zero in on the cause. The pool specific
// stats are provided separately.
type ZepClusterCollector struct {
	addrs       []string
	MetaCount   prometheus.Gauge
	NodeCount   prometheus.Gauge
	UpNodeCount prometheus.Gauge
	NodeUp      *prometheus.GaugeVec
	TableUsed   *prometheus.GaugeVec
	TableRemain *prometheus.GaugeVec
	TableQuery  *prometheus.GaugeVec
	TableQPS    *prometheus.GaugeVec
	TableOffset *prometheus.GaugeVec
	Epoch       *prometheus.GaugeVec
}

// NewClusterUsageCollector creates and returns the reference to ClusterUsageCollector
// and internally defines each metric that display cluster stats.
//func NewZepClusterCollector(conn Conn) *ZepClusterCollector {
func NewZepClusterCollector() *ZepClusterCollector {
	return &ZepClusterCollector{
		MetaCount: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "meta_count",
				Help:      "zeppelin meta server count",
			}),
		NodeCount: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "node_count",
				Help:      "zeppelin node server count",
			}),
		UpNodeCount: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "up_node_count",
				Help:      "zeppelin up node server count",
			}),
		NodeUp: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "NodeUp",
				Help:      "zeppelin node is up",
			},
			[]string{"node", "port"},
		),
		TableUsed: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "TableUsed",
				Help:      "zeppelin Table space used",
			},
			[]string{"table"},
		),
		TableRemain: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "TableRemain",
				Help:      "zeppelin Table space remain",
			},
			[]string{"table"},
		),
		TableQuery: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "TableQuery",
				Help:      "zeppelin Table Query",
			},
			[]string{"table"},
		),
		TableQPS: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "TableQPS",
				Help:      "zeppelin Table QPS",
			},
			[]string{"table"},
		),
		TableOffset: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "TableOffset",
				Help:      "zeppelin Table Offset",
			},
			[]string{"table", "partition", "addr"},
		),
		Epoch: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "Epoch",
				Help:      "zeppelin epoch",
			},
			[]string{"type", "table", "addr"},
		),
	}
}

func (c *ZepClusterCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		c.MetaCount,
		c.NodeCount,
		c.UpNodeCount,
		c.NodeUp,
		c.TableUsed,
		c.TableRemain,
		c.TableQuery,
		c.TableQPS,
		c.TableOffset,
		c.Epoch,
	}
}

//TODO 似乎跑了两次？
func (c *ZepClusterCollector) collect() error {
	var wg sync.WaitGroup
	rawNodes, err := zeppelin.ListNode()
	if err == nil {
		go func() {
			wg.Add(1)
			nodes := len(rawNodes)
			c.NodeCount.Set(float64(nodes))

			logger.Info("nodecount done")
			upnodes := 0
			for _, node := range rawNodes {
				if node.GetStatus() == 0 {
					c.NodeUp.WithLabelValues(node.Node.GetIp(), strconv.Itoa(int(node.Node.GetPort()))).Set(float64(node.GetStatus()))
					upnodes += 1
				} else {
					c.NodeUp.WithLabelValues(node.Node.GetIp(), strconv.Itoa(int(node.Node.GetPort()))).Set(float64(node.GetStatus()))
				}
			}
			c.UpNodeCount.Set(float64(upnodes))
			wg.Done()
			logger.Info("upnode done")
		}()
	}
	go func() {
		wg.Add(1)
		rawMetas, err := zeppelin.ListMeta(c.addrs)
		if err == nil {
			metas := len(rawMetas.GetFollowers()) + 1
			c.MetaCount.Set(float64(metas))
		}
		wg.Done()
		logger.Info("listMeta done")
	}()

	// listable --> space
	tablelist, err := zeppelin.ListTable()
	if err != nil {
		wg.Wait()
		return nil
		logger.Error("listtable error")
	}

	for _, tablename := range tablelist.Name {
		logger.Info("table info starting: ", tablename)
		ptable, err := zeppelin.PullTable(tablename, rawNodes)
		if err != nil {
			logger.Error("pull table error", err)
		}
		logger.Info("pulltable done")
		go func(tablename string, ptable zeppelin.PTable) {
			wg.Add(1)
			used, remain, _ := ptable.Space(tablename)
			c.TableUsed.WithLabelValues(tablename).Set(float64(used))
			c.TableRemain.WithLabelValues(tablename).Set(float64(remain))
			logger.Info("tableused tableremain done")

			tableEpoch := ptable.TableEpoch
			c.Epoch.WithLabelValues("table", tablename, "").Set(float64(tableEpoch))
			nodeEpoch, _ := ptable.Server()
			for _, e := range nodeEpoch {
				c.Epoch.WithLabelValues("node", "", e.Addr).Set(float64(e.Epoch))
			}
			logger.Info("epoch done")

			query, qps, _ := ptable.Stats(tablename)
			c.TableQuery.WithLabelValues(tablename).Set(float64(query))
			c.TableQPS.WithLabelValues(tablename).Set(float64(qps))
			logger.Info("query qps done")

			Offset, _ := ptable.Offset(tablename)
			for pid, slave := range Offset {
				for _, offset := range slave {
					c.TableOffset.WithLabelValues(tablename, strconv.Itoa(int(pid)), offset.Addr).Set(offset.Offset)
				}
			}
			logger.Info("offset done")
			wg.Done()
		}(tablename, ptable)
		logger.Info("table info end: ", tablename)
	}
	wg.Wait()
	logger.Info("alldone")
	go zeppelin.CloseAllConn()
	return nil
}

// Describe sends the descriptors of each metric over to the provided channel.
// The corresponding metric values are sent separately.
func (c *ZepClusterCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range c.collectorList() {
		metric.Describe(ch)
	}
}

// Collect sends the metric values for each metric pertaining to the global
// cluster usage over to the provided prometheus Metric channel.
func (c *ZepClusterCollector) Collect(ch chan<- prometheus.Metric) {
	if err := c.collect(); err != nil {
		logger.Error("failed collecting cluster usage metrics:", err)
		return
	}
	for _, metric := range c.collectorList() {
		metric.Collect(ch)
	}
}
