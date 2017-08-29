// Copyright © 2017 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"github.com/tinytub/zep-exporter/zeppelin"

	"github.com/spf13/cobra"
	"github.com/tinytub/zep-exporter/exporter"
)

// exporterCmd represents the exporter command
var exporterCmd = &cobra.Command{
	Use:   "exporter",
	Short: "zep exporter for prometheus",
	Run: func(cmd *cobra.Command, args []string) {
		//check region 并获取meta地址这个是不是要换个地方？
		meta := checkZepRegionNGetMeta(region)
		zeppelin.InitMetaConns(meta)
		zeppelin.InitNodeConns()
		go zeppelin.RefreashConns()
		exporter.DoExporter(addr, matricPath, hostType)
	},
}
var (
	addr       string
	matricPath string
	hostType   string
	region     string
)

func init() {

	exporterCmd.Flags().StringVar(&addr, "addr", ":9128", "listen address")
	exporterCmd.Flags().StringVar(&matricPath, "matricpath", "/metrics", "metric path")
	exporterCmd.Flags().StringVar(&hostType, "hosttype", "", "host type")
	//	exporterCmd.Flags().StringVar(&region, "region", "", "zep region")

	exporterCmd.PersistentFlags().StringVar(&region, "region", "", "zeppelin region")

	RootCmd.AddCommand(exporterCmd)

}
