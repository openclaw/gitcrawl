package cli

func normalizeCommandArgs(args []string, stringFlags map[string]bool) []string {
	var flags []string
	var positionals []string
	for index := 0; index < len(args); index++ {
		arg := args[index]
		name, ok := flagName(arg)
		if !ok {
			positionals = append(positionals, arg)
			continue
		}
		flags = append(flags, arg)
		for i := 0; i < len(name); i++ {
			if name[i] == '=' {
				name = name[:i]
				break
			}
		}
		if stringFlags[name] && !hasInlineValue(arg) && index+1 < len(args) {
			index++
			flags = append(flags, args[index])
		}
	}
	return append(flags, positionals...)
}

func flagName(arg string) (string, bool) {
	if len(arg) >= 3 && arg[:2] == "--" {
		return arg[2:], true
	}
	if len(arg) >= 2 && arg[0] == '-' && arg[1] != '-' {
		return arg[1:], true
	}
	return "", false
}

func hasInlineValue(arg string) bool {
	for i := 0; i < len(arg); i++ {
		if arg[i] == '=' {
			return true
		}
	}
	return false
}
