// Command dbspa-gen generates synthetic data fixtures for testing DBSPA.
package main

import (
	"fmt"
	"os"

	"github.com/alecthomas/kong"
)

func main() {
	var cli GenerateCmd
	kong.Parse(&cli,
		kong.Name("dbspa-gen"),
		kong.Description("Generate synthetic data fixtures for testing DBSPA."),
		kong.UsageOnError(),
		kong.ConfigureHelp(kong.HelpOptions{Compact: true}),
	)

	if err := cli.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
