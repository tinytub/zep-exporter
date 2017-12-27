package collectors

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

//var boolchange = map[string]int{"true": 0, "false": 1}

type ZepClusterS3Collector struct {
	addr                      string
	QPS                       prometheus.Gauge
	ClusterTraffic            prometheus.Counter
	ClusterRequestCount       prometheus.Counter
	ClusterFailedRequestCount prometheus.Counter
	ClusterAuthFailedCount    prometheus.Counter
	BucketTraffic             *prometheus.CounterVec
	BucketVolume              *prometheus.CounterVec
	CommandsInfo              *prometheus.CounterVec
}

func NewZepClusterS3Collector(path string) *ZepClusterS3Collector {
	if path == "" {
		fmt.Println("no remote path")
		os.Exit(1)
	}
	return &ZepClusterS3Collector{
		addr: path,
		QPS: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "QPS",
				Help:      "S3 cluster QPS",
			},
		),
		ClusterTraffic: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "ClusterTraffic",
				Help:      "S3 cluster traffic",
			},
		),
		ClusterRequestCount: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "ClusterRequestCount",
				Help:      "S3 cluster requsts failed count",
			},
		),
		ClusterFailedRequestCount: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "ClusterFailedRequestCount",
				Help:      "S3 cluster failed request",
			},
		),
		ClusterAuthFailedCount: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "ClusterAuthFailedCount",
				Help:      "S3 cluster auth failed count",
			},
		),
		BucketTraffic: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "BucketTraffic",
				Help:      "S3 bucket traffic",
			},
			[]string{"bucket"},
		),
		BucketVolume: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "BucketVolume",
				Help:      "S3 bucket volume usage",
			},
			[]string{"bucket"},
		),
		CommandsInfo: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "CommandsInfo",
				Help:      "S3 command infos",
			},
			[]string{"cmd", "state"},
		),
	}
}
func (c *ZepClusterS3Collector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		c.QPS,
		c.ClusterTraffic,
		c.ClusterRequestCount,
		c.ClusterFailedRequestCount,
		c.ClusterAuthFailedCount,
		c.BucketTraffic,
		c.BucketVolume,
		c.CommandsInfo,
	}
}

func (c *ZepClusterS3Collector) collect() error {
	//TODO url 合法检测
	client := http.Client{
		Timeout: time.Duration(3) * time.Second,
	}
	res, err := client.Get(c.addr)
	if err != nil {
		return err
	}
	data, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return err
	}

	var s3Status *S3Status
	err = json.Unmarshal(data, &s3Status)
	if err != nil {
		return err
	}

	if qps, err := strconv.Atoi(s3Status.QPS); err == nil {
		c.QPS.Set(float64(qps))
	}

	if ct, err := strconv.Atoi(s3Status.ClusterTraffic); err == nil {
		c.ClusterTraffic.Add(float64(ct))
	}

	if cr, err := strconv.Atoi(s3Status.RequestCount); err == nil {
		c.ClusterRequestCount.Add(float64(cr))
	}
	if cf, err := strconv.Atoi(s3Status.FailedRequest); err == nil {
		c.ClusterFailedRequestCount.Add(float64(cf))
	}
	if caf, err := strconv.Atoi(s3Status.AuthFailed); err == nil {
		c.ClusterAuthFailedCount.Add(float64(caf))
	}
	for _, bi := range s3Status.BucketsInfo {
		c.BucketTraffic.WithLabelValues(bi.Name).Add(float64(bi.Traffic))
		c.BucketVolume.WithLabelValues(bi.Name).Add(float64(bi.Volume))
	}
	for _, ci := range s3Status.CommandsInfo {
		cmd := strings.TrimSuffix(ci.Cmd, ": ")
		if rc, err := strconv.Atoi(ci.RequestCount); err == nil {
			c.CommandsInfo.WithLabelValues(cmd, "total").Add(float64(rc))
		}
		if five, err := strconv.Atoi(ci.FiveXXError); err == nil {
			c.CommandsInfo.WithLabelValues(cmd, "5xx").Add(float64(five))
		}
		if four, err := strconv.Atoi(ci.FourXXError); err == nil {
			c.CommandsInfo.WithLabelValues(cmd, "4xx").Add(float64(four))
		}

	}
	return nil

}

func (c *ZepClusterS3Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range c.collectorList() {
		metric.Describe(ch)
	}
}

func (c *ZepClusterS3Collector) Collect(ch chan<- prometheus.Metric) {
	if err := c.collect(); err != nil {
		logger.Error("failed collecting cluster usage metrics:", err)
		return
	}
	for _, metric := range c.collectorList() {
		metric.Collect(ch)
	}
}

type S3Status struct {
	AuthFailed        string `json:"auth_failed"`
	AvgUploadPartTime string `json:"avg_upload_part_time"`
	BucketsInfo       []struct {
		Name    string `json:"name"`
		Traffic int    `json:"traffic"`
		Volume  int    `json:"volume"`
	} `json:"buckets_info"`
	ClusterTraffic string `json:"cluster_traffic"`
	CommandsInfo   []struct {
		FourXXError  string `json:"4xx_error"`
		FiveXXError  string `json:"5xx_error"`
		Cmd          string `json:"cmd"`
		RequestCount string `json:"request_count"`
	} `json:"commands_info"`
	FailedRequest string `json:"failed_request"`
	QPS           string `json:"qps"`
	RequestCount  string `json:"request_count"`
}
