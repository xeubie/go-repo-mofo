package main

import (
	"fmt"
	"os"

	"repodojo"
)

func main() {
	args := os.Args[1:]

	cwdPath, err := os.Getwd()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	runOpts := repodojo.RunOpts{
		Out: os.Stdout,
		Err: os.Stderr,
	}

	opts := repodojo.RepoOpts{
		Hash: repodojo.SHA1Hash,
	}

	if err := repodojo.Run(opts, args, cwdPath, runOpts); err != nil {
		os.Exit(1)
	}
}
