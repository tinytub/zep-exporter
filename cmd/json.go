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
	"github.com/spf13/cobra"
	"github.com/tinytub/zep-exporter/exporter"
)

// exporterCmd represents the exporter command
var jsonexporterCmd = &cobra.Command{
	Use:   "json",
	Short: "zep exporter json for prometheus",
	Run: func(cmd *cobra.Command, args []string) {
		//check region 并获取meta地址这个是不是要换个地方？
		exporter.DoExporter(addr, path, matricPath, hostType)
	},
}

var (
	path       string
	addr       string
	matricPath string
	hostType   string
	region     string
)

func init() {

	jsonexporterCmd.Flags().StringVar(&path, "remotepath", "", "remote s3 path")
	jsonexporterCmd.Flags().StringVar(&matricPath, "matricpath", "/metrics", "metric path")
	jsonexporterCmd.Flags().StringVar(&hostType, "hosttype", "json", "host type")
	jsonexporterCmd.Flags().StringVar(&addr, "addr", ":9128", "listen address")

	RootCmd.AddCommand(jsonexporterCmd)
}
