// Command folddb-gen generates synthetic data fixtures for testing FoldDB.
package main

import (
	"fmt"
	"os"

	"github.com/alecthomas/kong"
)

func main() {
	var cli GenerateCmd
	kong.Parse(&cli,
		kong.Name("folddb-gen"),
		kong.Description("Generate synthetic data fixtures for testing FoldDB."),
		kong.UsageOnError(),
	)

	if err := cli.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
