package kafka

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dropbox/kafka/proto"
	"github.com/jpillora/backoff"
)

// DistributingProducer is the interface similar to Producer, but never require
// to explicitly specify partition.
//
// Distribute writes messages to the given topic, automatically choosing
// partition, returning the post-commit offset and any error encountered. The
// offset of each message is also updated accordingly.
type DistributingProducer interface {
	Distribute(topic string, messages ...*proto.Message) (partition int32, offset int64, err error)
}

// PartitionCountSource lets a DistributingProducer determine how many
// partitions exist for a particular topic. Broker fulfills this interface
// but a cache could be used instead.
type PartitionCountSource interface {
	PartitionCount(topic string) (count int32, err error)
}

// ErrorAverseRRProducerOpts controls the behavior of errorAverseRRProducer.
// PartitionCountSource: required
// Producer: required
// ErrorAverseBackoff: optional. Controls how long we should wait after a
// produce fails to a particular partition.
// PartitionFetchTimeout: optional. Controls how long Distribute will wait
// to get a partition in the case where they are all unavailable due to
// error averse backoff.
type errorAverseRRProducerConf struct {
	PartitionCountSource  PartitionCountSource
	Producer              Producer
	ErrorAverseBackoff    *backoff.Backoff
	PartitionFetchTimeout time.Duration
}

func NewErrorAverseRRProducerConf() *errorAverseRRProducerConf {
	return &errorAverseRRProducerConf{
		PartitionCountSource: nil,
		Producer:             nil,
		ErrorAverseBackoff: &backoff.Backoff{
			Min:    time.Duration(10 * time.Second),
			Max:    time.Duration(5 * time.Minute),
			Factor: 2, // Avoids race condition, see https://github.com/jpillora/backoff/pull/5
			Jitter: true,
		},
		PartitionFetchTimeout: time.Duration(10 * time.Second),
	}
}

// errorAverseRRProducer writes to a topic's partitions in order sequentially
// (stateful round robin) but when a produce fails, that partition is set
// aside temporarily using exponential backoff.
type errorAverseRRProducer struct {
	partitionCountSource PartitionCountSource
	producer             Producer
	partitionManager     *partitionManager
}

type NoPartitionsAvailable struct{}

func (NoPartitionsAvailable) Error() string {
	return "No partitions available within timeout."
}

func NewErrorAverseRRProducer(conf *errorAverseRRProducerConf) DistributingProducer {
	return &errorAverseRRProducer{
		partitionCountSource: conf.PartitionCountSource,
		producer:             conf.Producer,
		partitionManager: &partitionManager{
			availablePartitions: make(map[string]chan *partitionData),
			lock:                &sync.RWMutex{},
			sharedRetry:         conf.ErrorAverseBackoff,
			getTimeout:          conf.PartitionFetchTimeout,
		}}
}

func (d *errorAverseRRProducer) Distribute(topic string, messages ...*proto.Message) (
	partition int32, offset int64, err error) {

	if count, err := d.partitionCountSource.PartitionCount(topic); err == nil {
		d.partitionManager.SetPartitionCount(topic, count)
	} else {
		// This topic doesn't exist, so we pretend it has one partition for now.
		d.partitionManager.SetPartitionCount(topic, 1)
	}

	partitionData, err := d.partitionManager.GetPartition(topic)
	if err != nil {
		log.Error(err.Error())
		return 0, 0, &NoPartitionsAvailable{}
	}
	// We are now obligated to call Success or Failure on partitionData.
	if offset, err := d.producer.Produce(topic, partitionData.Partition, messages...); err != nil {
		err = fmt.Errorf("Failed to produce [%s:%d]: %s", topic, partitionData.Partition, err)
		log.Error(err.Error())
		partitionData.Failure()
		return 0, 0, err
	} else {
		partitionData.Success()
		return partitionData.Partition, offset, nil
	}
}

// partitionData wraps a retry tracker and the partitionManager's chan for
// a particular partition. We have a pointer to the chan instead of the
// partitionManager because the partitionManager will throw away and rebuild
// its availablePartitions chan whenever the partition count changes. This means
// calls to stale partitionData objects will manipulate a defunct
// availablePartitions chan, which is harmless.
//
// successiveFailures is meant to atomically track the most recent count of
// calls to Failure without an intervening call to Success. The implementation
// is somewhat racy because it does not use a mutex, which means that in the
// worst case of a partition which is intermittently unavailable, we may
// suspend the partition for more or less time than we would have in a fully
// synchronized implementation. However, in the steady states of healthy
// or fully down, this implementation is more performant.
type partitionData struct {
	Partition           int32
	sharedRetry         *backoff.Backoff
	successiveFailures  uint64
	availablePartitions chan *partitionData
	topic               string // Just for debugging
}

func (d *partitionData) Success() {
	// This logging message is quite racy, but is still useful because it indicates
	// a successful produce definitely happened in some sort of proximity to
	// a failed produce.
	if successiveFailures := atomic.LoadUint64(&d.successiveFailures); successiveFailures > 0 {
		log.Infof("Resetting partition successiveFailures for %d of %s, was %d",
			d.Partition, d.topic, successiveFailures)
	}
	atomic.StoreUint64(&d.successiveFailures, 0)
}

func (d *partitionData) Failure() {
	atomic.AddUint64(&d.successiveFailures, 1)
}

// reEnqueue makes this partition available for another producer thread, either
// immediately or after a sleep corresponding to the number of successiveFailures
// seen.
//
// While the inner function is running/sleeping, there may be other produces
// to this partition in flight, but no new producer threads can get this
// partition out of GetPartition.
func (d *partitionData) reEnqueue() {
	go func() {
		if successiveFailures := atomic.LoadUint64(&d.successiveFailures); successiveFailures > 0 {
			// The interface to ForAttempt is that the first failure should be #0.
			t := d.sharedRetry.ForAttempt(float64(successiveFailures - 1))
			log.Warningf("Suspending partition %d of %s for %s (%d)",
				d.Partition, d.topic, t, successiveFailures)
			time.Sleep(t)
			log.Warningf("Re-enqueueing partition %d of %s after %s",
				d.Partition, d.topic, t)
		}
		select {
		case d.availablePartitions <- d:
		default:
			log.Errorf("Programmer error in reEnqueue(%s, %d)! This should never happen.",
				d.topic, d.Partition)
		}
	}()
}

// partitionManager wraps the current availablePartitions which it
// rebuilds in response to changes in partition counts.
// The partitionManager also keeps a single Backoff object for shared use
// among all the partitionData objects that will exist.
type partitionManager struct {
	availablePartitions map[string]chan *partitionData
	lock                *sync.RWMutex
	sharedRetry         *backoff.Backoff
	getTimeout          time.Duration
}

// GetPartitionCount returns the size of a topic's availablePartitions chan.
func (p *partitionManager) GetPartitionCount(topic string) (int32, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	if availablePartitions, ok := p.availablePartitions[topic]; !ok {
		return 0, fmt.Errorf("No such topic %s", topic)
	} else {
		return int32(cap(availablePartitions)), nil
	}
}

// SetPartitionCount resets the partitionManager's state for the given topic
// if the count has changed. Partitions currently in error-averse backoff
// will immediately be available for writing again.
func (p *partitionManager) SetPartitionCount(topic string, partitionCount int32) {
	if count, err := p.GetPartitionCount(topic); err == nil && count == partitionCount {
		return
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	if availablePartitions, ok := p.availablePartitions[topic]; ok && int32(cap(availablePartitions)) == partitionCount {
		log.Errorf("partitionManager(%s) hit slow path on SetPartitionCount but "+
			"there is now no work to do. Count %d", topic, partitionCount)
		return
	} else {
		log.Infof("partitionManager adjusting partition count for %s: %d -> %d",
			topic, cap(availablePartitions), partitionCount)

		availablePartitions = make(chan *partitionData, partitionCount)
		for i := int32(0); i < partitionCount; i++ {
			availablePartitions <- &partitionData{
				Partition:           i,
				sharedRetry:         p.sharedRetry,
				availablePartitions: availablePartitions,
				topic:               topic,
			}
		}
		p.availablePartitions[topic] = availablePartitions
	}
}

// GetPartition fetches the next available partitionData object for the
// given topic. The caller must call Success or Failure on this partitionData.
func (p *partitionManager) GetPartition(topic string) (*partitionData, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	if availablePartitions, ok := p.availablePartitions[topic]; !ok {
		return nil, fmt.Errorf("No such topic %s", topic)
	} else {
		select {
		case partitionData, ok := <-availablePartitions:
			if !ok {
				return nil, fmt.Errorf(fmt.Sprintf("Programmer error in GetPartition(%s)! "+
					"This should never happen.", topic))
			}
			defer partitionData.reEnqueue()
			return partitionData, nil
		case <-time.After(p.getTimeout):
			return nil, fmt.Errorf(fmt.Sprintf("Timeout waiting for partition for %s.", topic))
		}
	}
}
