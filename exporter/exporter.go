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

// Command Zep_exporter provides a Prometheus exporter for a Zep cluster.
package exporter

import (
	"log"
	"net/http"
	"sync"

	"github.com/tinytub/zep-exporter/exporter/collectors"

	"github.com/prometheus/client_golang/prometheus"
)

// ZepExporter wraps all the Zep collectors and provides a single global
// exporter to extracts metrics out of. It also ensures that the collection
// is done in a thread-safe manner, the necessary requirement stated by
// prometheus. It also implements a prometheus.Collector interface in order
// to register it correctly.
type ZepExporter struct {
	mu         sync.Mutex
	collectors []prometheus.Collector
}

// Verify that the exporter implements the interface correctly.
var _ prometheus.Collector = &ZepExporter{}

// NewZepExporter creates an instance to ZepExporter and returns a reference
// to it. We can choose to enable a collector to extract stats out of by adding
// it to the list of collectors.
func NewZepExporter(hostType, path string) *ZepExporter {
	var exporter *ZepExporter
	switch hostType {
	case "json":
		exporter = &ZepExporter{
			collectors: []prometheus.Collector{
				collectors.NewZepClusterJsonCollector(),
			},
		}
	case "s3":
		exporter = &ZepExporter{
			collectors: []prometheus.Collector{
				collectors.NewZepClusterS3Collector(path),
			},
		}
	}
	return exporter
}

// Describe sends all the descriptors of the collectors included to
// the provided channel.
func (c *ZepExporter) Describe(ch chan<- *prometheus.Desc) {
	for _, cc := range c.collectors {
		cc.Describe(ch)
	}
}

// Collect sends the collected metrics from each of the collectors to
// prometheus. Collect could be called several times concurrently
// and thus its run is protected by a single mutex.
func (c *ZepExporter) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, cc := range c.collectors {
		cc.Collect(ch)
	}
}

func DoExporter(addr, path, metricsPath, hostType string) {

	prometheus.MustRegister(NewZepExporter(hostType, path))

	http.Handle(metricsPath, prometheus.Handler())

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, metricsPath, http.StatusMovedPermanently)
	})

	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("cannot start zep exporter: %s", err)
	}

}
