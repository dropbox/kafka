package kafkatest

import (
	"sync"

	"github.com/op/go-logging"
)

var log *logging.Logger
var logMu = &sync.Mutex{}

func init() {
	logMu.Lock()
	defer logMu.Unlock()

	if log != nil {
		return
	}
	log = logging.MustGetLogger("KafkaTest")
	logging.SetLevel(logging.INFO, "KafkaTest")
}

func SetLogger(l *logging.Logger) {
	logMu.Lock()
	defer logMu.Unlock()

	log = l
}
