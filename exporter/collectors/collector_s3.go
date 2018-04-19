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
	"github.com/tinytub/zep-exporter/s3core"
	"github.com/tinytub/zep-exporter/utils/system"
)

//var boolchange = map[string]int{"true": 0, "false": 1}

type ZepClusterS3Collector struct {
	Region                    string
	addr                      string
	ProcessUp                 prometheus.Gauge
	StatusAPI                 prometheus.Gauge
	S3ClusterUp               prometheus.Gauge
	QPS                       prometheus.Gauge
	ClusterTraffic            prometheus.Gauge
	ClusterRequestCount       prometheus.Gauge
	ClusterFailedRequestCount prometheus.Gauge
	ClusterAuthFailedCount    prometheus.Gauge
	BucketTraffic             *prometheus.GaugeVec
	BucketVolume              *prometheus.GaugeVec
	CommandsInfo              *prometheus.GaugeVec
}

func NewZepClusterS3Collector(path, region string) *ZepClusterS3Collector {
	if path == "" {
		logger.Error("there is no remote S3 metrics path found")
		os.Exit(1)
	}
	return &ZepClusterS3Collector{
		addr:   path,
		Region: region,
		ProcessUp: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "ProcessUp",
				Help:      "S3 gateway ProcessUp",
			},
		),
		StatusAPI: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "StatusAPI",
				Help:      "S3 gateway Status API up",
			},
		),
		S3ClusterUp: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "S3ClusterUp",
				Help:      "S3 gateway cluster up",
			},
		),
		QPS: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "QPS",
				Help:      "S3 cluster QPS",
			},
		),
		ClusterTraffic: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "ClusterTraffic",
				Help:      "S3 cluster traffic",
			},
		),
		ClusterRequestCount: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "ClusterRequestCount",
				Help:      "S3 cluster requsts failed count",
			},
		),
		ClusterFailedRequestCount: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "ClusterFailedRequestCount",
				Help:      "S3 cluster failed request",
			},
		),
		ClusterAuthFailedCount: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "ClusterAuthFailedCount",
				Help:      "S3 cluster auth failed count",
			},
		),
		BucketTraffic: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "BucketTraffic",
				Help:      "S3 bucket traffic",
			},
			[]string{"bucket"},
		),
		BucketVolume: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "BucketVolume",
				Help:      "S3 bucket volume usage",
			},
			[]string{"bucket"},
		),
		CommandsInfo: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
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
		c.ProcessUp,
		c.StatusAPI,
		c.S3ClusterUp,
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
	s3info := map[string]map[string]string{
		"shbtS3":   map[string]string{"key": "3VOvhEIJzOLfDeMLQNUw", "secret": "YH1eeGeCplAawtRUrwIaM5VZPCbvE3vwxzO4fCv5", "domain": "http://shbt.s3.addops.soft.360.cn"},
		"bjytS3":   map[string]string{"key": "acbQjR4IOLsARSOkmN2L", "secret": "HQTVLHkj8jb0PJqsQ0zlYEI1dkyRmqDnAohsGk8h", "domain": "http://s3.bjyt.addops.soft.360.cn"},
		"bjccS3":   map[string]string{"key": "K4KCqZLWEPKZBgcMAMWE", "secret": "Mnwz2pCvw4rgAnMuP9x3wNNVHWCnf1PhsgMRp8zt", "domain": "http://s3.bjcc.addops.soft.360.cn"},
		"sozzzcS3": map[string]string{"key": "nTwk5F7RDiVRFCEd5oUq", "secret": "SHlYTkGLDU1P8Owwk1llBqQXQ9niYLSWbjr8pcQs", "domain": "http://so-zzzc.s3.addops.soft.360.cn"},
		"shyc2S3":  map[string]string{"key": "VkPiPZQzD3ZOTqsYwVks", "secret": "53MIIzqTYnd7DPUzx6YFwXBxVDWWAoIW2yGVGnzk", "domain": "http://shyc2.s3.addops.soft.360.cn"},
		"latoS3":   map[string]string{"key": "01SkhYicJkcXISoFucbM", "secret": "qaNTjHGlXnGiykUvhm8Svv4lSzqx40RdaZzzCgqn", "domain": "http://104.192.110.232"},
		"zzzcS3":   map[string]string{"key": "s2rpv7dXQDf9Cw1CRKEz", "secret": "kJZHf0DqHWpn1yakUkS3dTaRDXAJOvbBcnMsZPCm", "domain": "http://zzzc.s3.addops.soft.360.cn"},
	}

	s3cli := s3core.NewClient(s3info[c.Region]["domain"], s3info[c.Region]["key"], s3info[c.Region]["secret"])

	if res := s3core.SetOBJ(s3cli, "s3-monitor", "s3-exporter", strconv.FormatInt(time.Now().UnixNano(), 10)); res {
		c.S3ClusterUp.Set(float64(1))
	} else {
		c.S3ClusterUp.Set(float64(0))
	}

	mstats := &system.Stats{
		Procs:        []string{"zgw_server"},
		CacheCmdLine: true,
	}

	err := mstats.Init()
	if err != nil {
		fmt.Println("mstat init error")
		return nil
	}

	mstats.GetProcMap()
	c.ProcessUp.Set(float64(0))
	for _, proc := range mstats.ProcsMap {
		if proc.Name == "zgw_server" {
			c.ProcessUp.Set(float64(1))
		}
	}

	//TODO url 合法检测
	client := http.Client{
		Timeout: time.Duration(3) * time.Second,
	}
	c.StatusAPI.Set(float64(1))
	res, err := client.Get(c.addr)
	if err != nil {
		c.StatusAPI.Set(float64(0))
		fmt.Println("curl api error")
		return nil
	}
	data, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		c.StatusAPI.Set(float64(0))
		fmt.Println("read body error")
		return nil
	}

	var s3Status *S3Status
	err = json.Unmarshal(data, &s3Status)
	if err != nil {
		return err
	}

	if qps, err := strconv.Atoi(s3Status.QPS); err == nil {
		c.QPS.Set(float64(qps))
	} else {
		logger.Errorf("failed to convert QPS to int, error: %s", err)
	}

	if ct, err := strconv.Atoi(s3Status.ClusterTraffic); err == nil {
		c.ClusterTraffic.Set(float64(ct))
	} else {
		logger.Errorf("failed to convert ClusterTraffic to int, error: %s", err)
	}

	if cr, err := strconv.Atoi(s3Status.RequestCount); err == nil {
		c.ClusterRequestCount.Set(float64(cr))
	} else {
		logger.Errorf("failed to convert RequestCount to int, error: %s", err)
	}
	if cf, err := strconv.Atoi(s3Status.FailedRequest); err == nil {
		c.ClusterFailedRequestCount.Set(float64(cf))
	} else {
		logger.Errorf("failed to convert FailedRequest to int, error: %s", err)
	}
	if caf, err := strconv.Atoi(s3Status.AuthFailed); err == nil {
		c.ClusterAuthFailedCount.Set(float64(caf))
	} else {
		logger.Errorf("failed to convert AuthFailed to int, error: %s", err)
	}
	for _, bi := range s3Status.BucketsInfo {
		c.BucketTraffic.WithLabelValues(bi.Name).Set(float64(bi.Traffic))
		c.BucketVolume.WithLabelValues(bi.Name).Set(float64(bi.Volume))
	}
	for _, ci := range s3Status.CommandsInfo {
		cmd := strings.TrimSuffix(ci.Cmd, ": ")
		if rc, err := strconv.Atoi(ci.RequestCount); err == nil {
			c.CommandsInfo.WithLabelValues(cmd, "total").Set(float64(rc))
		} else {
			logger.Errorf("failed to convert cmdinfo RequestCount to int, error: %s", err)
		}
		if five, err := strconv.Atoi(ci.FiveXXError); err == nil {
			c.CommandsInfo.WithLabelValues(cmd, "5xx").Set(float64(five))
		} else {
			logger.Errorf("failed to convert cmdinfo 5xx to int, error: %s", err)
		}
		if four, err := strconv.Atoi(ci.FourXXError); err == nil {
			c.CommandsInfo.WithLabelValues(cmd, "4xx").Set(float64(four))
		} else {
			logger.Errorf("failed to convert cmdinfo 4xx to int, error: %s", err)
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
