package syssig

import (
	"context"
	"fmt"
	"github.com/apache/servicecomb-service-center/pkg/log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

var (
	once       sync.Once
	lock       sync.RWMutex
	handlerMap = map[os.Signal][]func(){
		syscall.SIGHUP:  {},
		syscall.SIGINT:  {},
		syscall.SIGKILL: {},
		syscall.SIGTERM: {},
	}
)

func Run(ctx context.Context) {
	once.Do(func() {
		listenSignals := make([]os.Signal, 0, 10)
		for key := range handlerMap {
			listenSignals = append(listenSignals, key)
		}

		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, listenSignals...)

		select {
		case <-ctx.Done():

		case sig := <-sigChan:
			log.Infof("system signal: %s", sig.String())
			calls := callbacks(sig)
			for _, call := range calls {
				call()
			}
		}

	})
}

func AddSignalsHandler(handler func(), signals ...os.Signal) error {
	for _, sig := range signals {
		lock.RLock()
		handlers, ok := handlerMap[sig]
		lock.RUnlock()
		if !ok {
			return fmt.Errorf("system signal %s is not notify", sig.String())
		}
		handlers = append(handlers, handler)
		lock.Lock()
		handlerMap[sig] = handlers
		lock.Unlock()
	}
	return nil
}

func callbacks(signal os.Signal) []func() {
	lock.RLock()
	calls := handlerMap[signal]
	lock.RUnlock()
	return calls
}
