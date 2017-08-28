package main

import (
	"github.com/tinytub/zep-exporter/cmd"
	"github.com/tinytub/zep-exporter/logging"
)

func main() {
	logging.Configure()
	cmd.Execute()
}
