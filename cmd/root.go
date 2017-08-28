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
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgFile string

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "zep-cli",
	Short: "A brief description of your application",
	Long: `A longer description that spans multiple lines and likely contains
examples and usage of using your application. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	//	Run: func(cmd *cobra.Command, args []string) { },
}

/*
var (
	region string
)
*/

// Execute adds all child commands to the root command sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	//RootCmd.Flags().StringVar(&region, "region", "", "cluster region")

	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	// Here you will define your flags and configuration settings.
	// Cobra supports Persistent Flags, which, if defined here,
	// will be global for your application.

	RootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.zep-cli.yaml)")
	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	RootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" { // enable ability to specify config file via flag
		viper.SetConfigFile(cfgFile)
	}

	lodir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatal(err)
	}

	viper.SetConfigName(".zep-cli") // name of config file (without extension)
	viper.AddConfigPath("$HOME/")   // adding home directory as first search path
	viper.AddConfigPath(lodir)
	viper.AutomaticEnv() // read in environment variables that match
	//fmt.Println(viper.ConfigFileUsed())
	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err != nil {
		fmt.Println(err)
	}
	//fmt.Println("Using config file:", viper.ConfigFileUsed())
}

func ListS3Region() {
	s3conf := viper.Get("s3")
	//fmt.Println(s3conf.(*map[string]map[string]string))
	if s3conf != nil {
		fmt.Println("can not find specified region")
		fmt.Println("find these region in config:")
		for key := range s3conf.(map[string]interface{}) {
			fmt.Println(key)
		}
	}
}

func ListZepRegion() {
	s3conf := viper.Get("zep")
	//fmt.Println(s3conf.(*map[string]map[string]string))
	if s3conf != nil {
		fmt.Println("can not find specified region")
		fmt.Println("find these region in config:")
		for key := range s3conf.(map[string]interface{}) {
			fmt.Println(key)
		}
	}
}

func getZepAllRegion() []string {
	conf := viper.Get("zep")
	var regionlist []string
	for region, _ := range conf.(map[string]interface{}) {
		regionlist = append(regionlist, region)
	}
	return regionlist
}

func checkZepRegionNGetMeta(region string) []string {
	//var meta []string
	path := fmt.Sprintf("zep.%s.meta", region)
	conf := viper.GetStringSlice(path)
	if conf == nil {
		ListZepRegion()
		os.Exit(0)
		//return nil, nil, nil
	}
	return conf
}
