package kafka

import (
	"flag"
	"sync"

	"github.com/op/go-logging"
)

var (
	log *logging.Logger
	logMu = &sync.Mutex{}

	logLevel = flag.Int(
		"kafka.log_level",
		int(logging.INFO),
		"Set logging verbosity for Kafka client.")
)

func init() {
	logMu.Lock()
	defer logMu.Unlock()

	if log != nil {
		return
	}
	log = logging.MustGetLogger("KafkaClient")
	logging.SetLevel(logging.Level(*logLevel), "KafkaClient")
}

func SetLogger(l *logging.Logger) {
	logMu.Lock()
	defer logMu.Unlock()

	log = l
}
