package cli

func normalizeCommandArgs(args []string, stringFlags map[string]bool) []string {
	var flags []string
	var positionals []string
	for index := 0; index < len(args); index++ {
		arg := args[index]
		if len(arg) < 2 || arg[:2] != "--" {
			positionals = append(positionals, arg)
			continue
		}
		flags = append(flags, arg)
		name := arg[2:]
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

func hasInlineValue(arg string) bool {
	for i := 0; i < len(arg); i++ {
		if arg[i] == '=' {
			return true
		}
	}
	return false
}
