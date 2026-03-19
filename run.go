package repodojo

import (
	"fmt"
	"io"
	"path/filepath"
)

type RunOpts struct {
	Out io.Writer
	Err io.Writer
}

var (
	ErrRepoNotFound     = fmt.Errorf("repo not found")
	ErrRepoAlreadyExists = fmt.Errorf("repo already exists")
	ErrHandled          = fmt.Errorf("handled error")
)

func Run(opts RepoOpts, args []string, cwdPath string, runOpts RunOpts) error {
	cmdArgs := ParseCommandArgs(args)
	dispatch := NewDispatch(cmdArgs)

	switch dispatch.Kind {
	case DispatchInvalidCommand:
		fmt.Fprintf(runOpts.Err, "\"%s\" is not a valid command\n\n", dispatch.InvalidName)
		PrintHelp(nil, runOpts.Err)
		return ErrHandled

	case DispatchInvalidArgument:
		fmt.Fprintf(runOpts.Err, "\"%s\" is not a valid argument\n\n", dispatch.InvalidName)
		PrintHelp(dispatch.InvalidCmd, runOpts.Err)
		return ErrHandled

	case DispatchHelp:
		PrintHelp(dispatch.HelpCmd, runOpts.Out)
		return nil

	case DispatchCLI:
		return runPrint(opts, dispatch.Command, cwdPath, runOpts)
	}
	return nil
}

func runPrint(opts RepoOpts, cmd *Command, cwdPath string, runOpts RunOpts) error {
	err := runCommand(opts, cmd, cwdPath, runOpts)
	if err == nil {
		return nil
	}
	switch err.Error() {
	case ErrRepoAlreadyExists.Error():
		fmt.Fprintf(runOpts.Err,
			"repo already exists, dummy.\n"+
				"two repos in the same directory makes no sense.\n"+
				"think about it.\n")
	case ErrRepoNotFound.Error():
		fmt.Fprintf(runOpts.Err,
			"repo not found, dummy.\n"+
				"either you're in the wrong place or you need to make a new one like this:\n\n")
		PrintHelp(&[]CommandKind{CommandInit}[0], runOpts.Err)
	default:
		return err
	}
	return ErrHandled
}

func runCommand(opts RepoOpts, cmd *Command, cwdPath string, runOpts RunOpts) error {
	switch cmd.Kind {
	case CommandInit:
		workPath, err := filepath.Abs(filepath.Join(cwdPath, cmd.Init.Dir))
		if err != nil {
			return err
		}
		repo, err := InitRepo(workPath, opts)
		if err != nil {
			return err
		}
		defer repo.Close()

		fmt.Fprintf(runOpts.Out,
			"congrats, you just created a new repo! aren't you special.\n"+
				"try setting your name and email like this:\n\n"+
				"    repodojo config add user.name foo\n"+
				"    repodojo config add user.email foo@bar\n")
		return nil
	}
	return fmt.Errorf("unknown command")
}
