package cli

import "fmt"

type cliError struct {
	code int
	err  error
}

func (e *cliError) Error() string {
	return e.err.Error()
}

func ExitCode(err error) int {
	if err == nil {
		return 0
	}
	if typed, ok := err.(*cliError); ok {
		return typed.code
	}
	return 1
}

func usageErr(err error) error {
	return &cliError{code: 2, err: err}
}

func notImplemented(command string) error {
	return fmt.Errorf("%s is not implemented yet", command)
}
