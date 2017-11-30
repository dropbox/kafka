package kafkatest

import (
	"flag"
	"sync"

	"github.com/op/go-logging"
)

var (
	log *logging.Logger
	logMu = &sync.Mutex{}

	logLevel = flag.Int(
		"kafkatest.log_level",
		int(logging.INFO),
		"Set logging verbosity for kafkatest package.")
)

func init() {
	logMu.Lock()
	defer logMu.Unlock()

	if log != nil {
		return
	}
	log = logging.MustGetLogger("KafkaTest")
	logging.SetLevel(logging.Level(*logLevel), "KafkaTest")
}

func SetLogger(l *logging.Logger) {
	logMu.Lock()
	defer logMu.Unlock()

	log = l
}
