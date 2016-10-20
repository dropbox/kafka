package kafka

import (
	"sync"

	"github.com/op/go-logging"

	. "gopkg.in/check.v1"
)

type logTestBackend struct {
	c  *C
	mu *sync.Mutex
}

var logTest = &logTestBackend{mu: &sync.Mutex{}}

func init() {
	logMu.Lock()
	defer logMu.Unlock()

	log = logging.MustGetLogger("KafkaClient")
	log.SetBackend(logging.AddModuleLevel(logTest))
	logging.SetLevel(logging.DEBUG, "KafkaClient")
}

func (l *logTestBackend) SetC(c *C) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.c = c
}

func ResetTestLogger(c *C) {
	logTest.SetC(c)
}

func (l *logTestBackend) Log(lvl logging.Level, cd int, rec *logging.Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.c.Log(rec.Formatted(cd))
	return nil
}
