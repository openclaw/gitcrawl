package main

import (
	"context"
	"os/signal"
)

func signalContext(parent context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(parent, commandSignals()...)
}
