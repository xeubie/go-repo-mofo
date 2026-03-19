package repodojo

import (
	"fmt"
	"io"
	"strings"
)

type CommandKind int

const (
	CommandInit CommandKind = iota
)

var commandNames = map[CommandKind]string{
	CommandInit: "init",
}

var commandDescrips = map[CommandKind]string{
	CommandInit: "create an empty repository.",
}

var commandExamples = map[CommandKind]string{
	CommandInit: `in the current dir:
    repodojo init
in a new dir:
    repodojo init myproject`,
}

// valueFlags are flags that can have a value associated with them.
var valueFlags = map[string]bool{
	"-m": true,
}

type CommandArgs struct {
	CommandKind    *CommandKind
	CommandName    *string
	PositionalArgs []string
	MapArgs        map[string]*string // nil value means flag present but no value
	UnusedArgs     map[string]bool
}

func ParseCommandArgs(args []string) *CommandArgs {
	var positionalArgs []string
	mapArgs := make(map[string]*string)
	unusedArgs := make(map[string]bool)
	var orderedKeys []string

	for _, arg := range args {
		if len(arg) > 1 && arg[0] == '-' {
			mapArgs[arg] = nil
			unusedArgs[arg] = true
			orderedKeys = append(orderedKeys, arg)
		} else {
			// if the last key is a value flag and doesn't have a value yet,
			// set this arg as its value
			if len(orderedKeys) > 0 {
				lastKey := orderedKeys[len(orderedKeys)-1]
				if valueFlags[lastKey] && mapArgs[lastKey] == nil {
					val := arg
					mapArgs[lastKey] = &val
					continue
				}
			}
			positionalArgs = append(positionalArgs, arg)
		}
	}

	ca := &CommandArgs{
		PositionalArgs: positionalArgs,
		MapArgs:        mapArgs,
		UnusedArgs:     unusedArgs,
	}

	if len(positionalArgs) == 0 {
		return ca
	}

	cmdName := positionalArgs[0]
	ca.CommandName = &cmdName
	ca.PositionalArgs = positionalArgs[1:]

	for kind, name := range commandNames {
		if name == cmdName {
			k := kind
			ca.CommandKind = &k
			break
		}
	}

	return ca
}

func (ca *CommandArgs) Contains(arg string) bool {
	delete(ca.UnusedArgs, arg)
	_, ok := ca.MapArgs[arg]
	return ok
}

type InitCommand struct {
	Dir string
}

type Command struct {
	Kind CommandKind
	Init *InitCommand
}

func parseCommand(cmdArgs *CommandArgs) *Command {
	if cmdArgs.CommandKind == nil {
		return nil
	}
	switch *cmdArgs.CommandKind {
	case CommandInit:
		if len(cmdArgs.PositionalArgs) == 0 {
			return &Command{Kind: CommandInit, Init: &InitCommand{Dir: "."}}
		} else if len(cmdArgs.PositionalArgs) == 1 {
			return &Command{Kind: CommandInit, Init: &InitCommand{Dir: cmdArgs.PositionalArgs[0]}}
		}
		return nil
	}
	return nil
}

type DispatchKind int

const (
	DispatchInvalidCommand  DispatchKind = iota
	DispatchInvalidArgument
	DispatchHelp
	DispatchCLI
)

type Dispatch struct {
	Kind        DispatchKind
	InvalidName string
	InvalidCmd  *CommandKind
	HelpCmd     *CommandKind
	Command     *Command
}

func NewDispatch(cmdArgs *CommandArgs) *Dispatch {
	showHelp := cmdArgs.Contains("--help")
	cmdArgs.Contains("--cli") // consume it

	if cmdArgs.CommandKind != nil {
		if showHelp {
			return &Dispatch{Kind: DispatchHelp, HelpCmd: cmdArgs.CommandKind}
		}
		if cmd := parseCommand(cmdArgs); cmd != nil {
			// check for unused args
			for arg := range cmdArgs.UnusedArgs {
				return &Dispatch{
					Kind:       DispatchInvalidArgument,
					InvalidCmd: cmdArgs.CommandKind,
					InvalidName: arg,
				}
			}
			return &Dispatch{Kind: DispatchCLI, Command: cmd}
		}
		return &Dispatch{Kind: DispatchHelp, HelpCmd: cmdArgs.CommandKind}
	} else if cmdArgs.CommandName != nil {
		return &Dispatch{Kind: DispatchInvalidCommand, InvalidName: *cmdArgs.CommandName}
	} else if showHelp {
		return &Dispatch{Kind: DispatchHelp}
	}
	return &Dispatch{Kind: DispatchHelp}
}

func PrintHelp(cmdKind *CommandKind, w io.Writer) {
	if cmdKind != nil {
		name := commandNames[*cmdKind]
		descrip := commandDescrips[*cmdKind]
		example := commandExamples[*cmdKind]
		fmt.Fprintf(w, "%s  %s\n\n", name, descrip)
		for _, line := range strings.Split(example, "\n") {
			fmt.Fprintf(w, "    %s\n", line)
		}
	} else {
		fmt.Fprintf(w, "help: repodojo <command> [<args>]\n\n")
		for kind, name := range commandNames {
			fmt.Fprintf(w, "%s  %s\n", name, commandDescrips[kind])
		}
	}
}
