package main

import (
	"fmt"
	"os"

	repomofo "github.com/xeubie/go-repo-mofo"
)

func main() {
	args := os.Args[1:]

	cwdPath, err := os.Getwd()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	runOpts := repomofo.RunOpts{
		Out: os.Stdout,
		Err: os.Stderr,
	}

	opts := repomofo.RepoOpts{
		Hash: repomofo.SHA1Hash,
	}

	if err := repomofo.RunPrint(opts, args, cwdPath, runOpts); err != nil {
		if err == repomofo.ErrHandled {
			os.Exit(1)
		}
		panic(err)
	}
}
