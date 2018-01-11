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
	"bufio"
	"encoding/json"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"sync"

	logging "github.com/op/go-logging"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace = "zeppelin"
)

var (
	logger     = logging.MustGetLogger("collector")
	boolchange = map[string]int{"true": 0, "false": 1}
)

// A ClusterUsageCollector is used to gather all the global stats about a given
// zep cluster. It is sometimes essential to know how fast the cluster is growing
// or shrinking as a whole in order to zero in on the cause. The pool specific
// stats are provided separately.
type ZepClusterJsonCollector struct {
	tableCount int

	MetaCount          prometheus.Gauge
	MetaInfo           *prometheus.GaugeVec
	CompleteProportion prometheus.Gauge
	NodeCount          prometheus.Gauge
	NodeUp             prometheus.Gauge
	NodeDown           prometheus.Gauge
	MetaMaster         *prometheus.GaugeVec
	TableUsed          *prometheus.GaugeVec
	TableRemain        *prometheus.GaugeVec
	TableQuery         *prometheus.GaugeVec
	TableQPS           *prometheus.GaugeVec
	TableCount         prometheus.Gauge
	Epoch              prometheus.Gauge
	Dismatch           prometheus.Gauge
	Healthy            prometheus.Gauge
	Pcount             *prometheus.GaugeVec
	Inconsistent       *prometheus.GaugeVec
	Incomplete         *prometheus.GaugeVec
	Lagging            *prometheus.GaugeVec
	Stuck              *prometheus.GaugeVec
	SlowDown           *prometheus.GaugeVec
}

// NewClusterUsageCollector creates and returns the reference to ClusterUsageCollector
// and internally defines each metric that display cluster stats.
//func NewZepClusterCollector(conn Conn) *ZepClusterCollector {
func NewZepClusterJsonCollector() *ZepClusterJsonCollector {
	return &ZepClusterJsonCollector{
		MetaCount: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "meta_count",
				Help:      "zeppelin meta server count",
			}),
		MetaInfo: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "meta_info",
				Help:      "zeppelin meta info",
			},
			[]string{"addr"},
		),
		CompleteProportion: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "complete_proportion",
				Help:      "zeppelin meta info",
			},
		),

		NodeCount: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "node_count",
				Help:      "zeppelin node server count",
			}),

		NodeDown: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "NodeDown",
				Help:      "zeppelin up node server count",
			}),
		NodeUp: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "NodeUp",
				Help:      "zeppelin node is up",
			},
		),

		MetaMaster: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "meta_master",
				Help:      "zeppelin meta server master",
			},
			[]string{"master"},
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
		TableCount: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "TableCount",
				Help:      "zeppelin Table Count",
			},
		),
		Epoch: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "Epoch",
				Help:      "zeppelin epoch",
			},
		),
		Dismatch: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "Dismatch",
				Help:      "zeppelin dismatch",
			},
		),
		Healthy: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "Healthy",
				Help:      "zeppelin healty",
			},
		),

		Pcount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "Pcount",
				Help:      "zeppelin pcount",
			},
			[]string{"table"},
		),
		Inconsistent: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "Inconsistent",
				Help:      "zeppelin inconsistent",
			},
			[]string{"table"},
		),
		Incomplete: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "Incomplete",
				Help:      "zeppelin incomplete",
			},
			[]string{"table"},
		),
		Lagging: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "Lagging",
				Help:      "zeppelin lagging",
			},
			[]string{"table"},
		),
		Stuck: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "Stuck",
				Help:      "zeppelin Stuck",
			},
			[]string{"table"},
		),
		SlowDown: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "Slowdown",
				Help:      "zeppelin slowdown",
			},
			[]string{"table"},
		),
	}
}

func (c *ZepClusterJsonCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		c.MetaCount,
		c.MetaInfo,
		c.CompleteProportion,
		c.NodeCount,
		c.NodeDown,
		c.MetaMaster,
		c.NodeUp,
		c.TableUsed,
		c.TableRemain,
		c.TableQuery,
		c.TableQPS,
		c.TableCount,
		c.Epoch,
		c.Dismatch,
		c.Healthy,
		c.Pcount,
		c.Inconsistent,
		c.Incomplete,
		c.Lagging,
		c.Stuck,
		c.SlowDown,
	}
}

//TODO 似乎跑了两次？
func (c *ZepClusterJsonCollector) collect() error {

	info, checkup, err := fileCheck()
	if err != nil {
		return err
	}

	if info.Meta.Error != "true" {
		c.MetaCount.Set(float64(info.Meta.Count))
		c.MetaMaster.Reset()
		c.MetaMaster.WithLabelValues(info.Meta.Leader.Node).Set(1)
		switch info.Meta.Leader.Status {
		case "Up":
			c.MetaInfo.WithLabelValues(info.Meta.Leader.Node).Set(0)
		case "Down":
			c.MetaInfo.WithLabelValues(info.Meta.Leader.Node).Set(1)
		}
		for _, fol := range info.Meta.Followers {
			switch fol.Status {
			case "Up":
				c.MetaInfo.WithLabelValues(fol.Node).Set(0)
			case "Down":
				c.MetaInfo.WithLabelValues(fol.Node).Set(1)
			}
		}
		c.CompleteProportion.Set(float64(info.Meta.CompleteProportion))
	}

	if info.Query.Error != "true" {
		tc := len(info.Query.Detail)
		if c.tableCount != tc {
			c.tableCount = tc
			c.TableUsed.Reset()
			c.TableRemain.Reset()
		}
		for _, table := range info.Query.Detail {
			c.TableQuery.WithLabelValues(table.Name).Set(float64(table.TotalQuery))
			c.TableQPS.WithLabelValues(table.Name).Set(float64(table.QPS))
		}
	}

	//TODO 这里可以对容量作更多计算
	if info.Space.Error != "true" {
		tc := len(info.Space.Detail)
		if c.tableCount != tc {
			c.tableCount = tc
			c.TableUsed.Reset()
			c.TableRemain.Reset()
		}

		for _, table := range info.Space.Detail {
			var used int = 0
			var remain int64 = 0
			for _, node := range table.Detail {
				remain += node.Remain
				used += node.Used
			}
			c.TableUsed.WithLabelValues(table.Name).Set(float64(used))
			c.TableRemain.WithLabelValues(table.Name).Set(float64(remain))
		}
	}

	c.Healthy.Set(float64(boolchange[checkup.Conclusion.Healthy]))

	if checkup.Node.Error != "true" {
		c.NodeCount.Set(float64(checkup.Node.Count))
		c.NodeUp.Set(float64(checkup.Node.Up))
		c.NodeDown.Set(float64(checkup.Node.Down))
	}
	if checkup.Epoch.Error != "true" {
		c.Epoch.Set(float64(checkup.Epoch.Epoch))
		c.Dismatch.Set(float64(checkup.Epoch.DismatchNum))
	}

	if checkup.Table.Error != "true" {
		c.TableCount.Set(float64(checkup.Table.Count))
		for _, table := range checkup.Table.Detail {
			if table.Result != "passed" {
				c.Pcount.WithLabelValues(table.Name).Set(float64(table.PCount))
				c.Inconsistent.WithLabelValues(table.Name).Set(float64(table.Inconsistent))
				c.Incomplete.WithLabelValues(table.Name).Set(float64(table.Incomplete))
				c.Lagging.WithLabelValues(table.Name).Set(float64(table.Lagging))
				c.Stuck.WithLabelValues(table.Name).Set(float64(table.Stuck))
				c.SlowDown.WithLabelValues(table.Name).Set(float64(table.Slowdown))
			} else {
				c.Pcount.Reset()
				c.Inconsistent.Reset()
				c.Incomplete.Reset()
				c.Lagging.Reset()
				c.Stuck.Reset()
				c.SlowDown.Reset()
			}
		}
	}

	return nil
}

// Describe sends the descriptors of each metric over to the provided channel.
// The corresponding metric values are sent separately.
func (c *ZepClusterJsonCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range c.collectorList() {
		metric.Describe(ch)
	}
}

// Collect sends the metric values for each metric pertaining to the global
// cluster usage over to the provided prometheus Metric channel.
func (c *ZepClusterJsonCollector) Collect(ch chan<- prometheus.Metric) {
	if err := c.collect(); err != nil {
		logger.Error("failed collecting cluster usage metrics:", err)
		return
	}
	for _, metric := range c.collectorList() {
		metric.Collect(ch)
	}
}

func exeCmd(cmd string, wg *sync.WaitGroup) error {
	wg.Add(1)
	parts := strings.Fields(cmd)
	head := parts[0]
	parts = parts[1:len(parts)]

	err := exec.Command(head, parts...).Run()
	if err != nil {
		return err
	}
	wg.Done()
	return nil
}

func fileCheck() (Info, Checkup, error) {
	var info Info
	var checkup Checkup

	infoFile, err := os.Open("/tmp/info_json_result")
	if err != nil {
		logger.Error("cannot open file", "/tmp/info_json_result")
		return info, checkup, err
	}
	defer infoFile.Close()

	infoReader := bufio.NewReader(infoFile)
	infoContent, _ := ioutil.ReadAll(infoReader)

	checkupFile, _ := os.Open("/tmp/checkup_json_result")
	if err != nil {
		logger.Error("cannot open file", "/tmp/checkup_json_result")
		return info, checkup, err
	}
	defer checkupFile.Close()

	checkupReader := bufio.NewReader(checkupFile)
	checkupContent, _ := ioutil.ReadAll(checkupReader)

	if err := json.Unmarshal(infoContent, &info); err != nil {
		logger.Error("json unmarshal error ", err)
		return info, checkup, err
	}
	if err := json.Unmarshal(checkupContent, &checkup); err != nil {
		logger.Error("json unmarshal error ", err)
		return info, checkup, err
	}

	return info, checkup, nil
}
