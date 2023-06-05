package main

// SIGUSR1 toggle the pause/resume consumption
import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/rcrowley/go-metrics"
	"log"
	"sync"

	_ "net/http/pprof"
)

// Sarama configuration options
var (
	brokers   = "127.0.0.1:9092"
	version   = "3.2.3"
	topic     = "saram"
	producers = 1 // 开启10个生产者进程发送数据
	verbose   = false

	recordsNumber int64 = 1

	recordsRate = metrics.GetOrRegisterMeter("records.rate", nil)
)

func init() {
	//flag.StringVar(&brokers, "brokers", "", "Kafka bootstrap brokers to connect to, as a comma separated list")
	//flag.StringVar(&version, "version", "0.11.0.0", "Kafka cluster version")
	//flag.StringVar(&topic, "topic", "", "Kafka topics where records will be copied from topics.")
	//flag.IntVar(&producers, "producers", 10, "Number of concurrent producers")
	//flag.Int64Var(&recordsNumber, "records-number", 10000, "Number of records sent per loop")
	//flag.BoolVar(&verbose, "verbose", false, "Sarama logging")
	//flag.Parse()

	if len(brokers) == 0 {
		log.Printf("no Kafka bootstrap brokers defined, please set the -brokers flag")
	}

	if len(topic) == 0 {
		log.Printf("no topic given to be consumed, please set the -topic flag")
	}
}

// 发送测试信息
func produceTestRecord(producerProvider *producerProvider) {
	producer := producerProvider.borrow()
	defer producerProvider.release(producer)

	// Start kafka transaction
	err := producer.BeginTxn()
	if err != nil {
		log.Printf("unable to start txn %s\n", err)
		return
	}

	// Produce some records in transaction
	var i int64
	for i = 0; i < recordsNumber; i++ {
		producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder("test")}
	}

	// commit transaction
	err = producer.CommitTxn()
	if err != nil {
		log.Printf("Producer: unable to commit txn %s\n", err)
		for {
			if producer.TxnStatus()&sarama.ProducerTxnFlagFatalError != 0 {
				// fatal error. need to recreate producer.
				log.Printf("Producer: producer is in a fatal state, need to recreate it")
				break
			}
			// If producer is in abortable state, try to abort current transaction.
			if producer.TxnStatus()&sarama.ProducerTxnFlagAbortableError != 0 {
				err = producer.AbortTxn()
				if err != nil {
					// If an error occured just retry it.
					log.Printf("Producer: unable to abort transaction: %+v", err)
					continue
				}
				break
			}
			// if not you can retry
			err = producer.CommitTxn()
			if err != nil {
				log.Printf("Producer: unable to commit txn %s\n", err)
				continue
			}
		}
		return
	}
	recordsRate.Mark(recordsNumber)
}

// pool of producers that ensure transactional-id is unique.
type producerProvider struct {
	transactionIdGenerator int32

	producersLock sync.Mutex
	producers     []sarama.AsyncProducer

	producerProvider func() sarama.AsyncProducer
}

func newProducerProvider(brokers []string, producerConfigurationProvider func() *sarama.Config) *producerProvider {
	provider := &producerProvider{}
	provider.producerProvider = func() sarama.AsyncProducer {
		config := producerConfigurationProvider()
		suffix := provider.transactionIdGenerator
		// Append transactionIdGenerator to current config.Producer.Transaction.ID to ensure transaction-id uniqueness.
		if config.Producer.Transaction.ID != "" {
			provider.transactionIdGenerator++
			config.Producer.Transaction.ID = config.Producer.Transaction.ID + "-" + fmt.Sprint(suffix)
		}
		producer, err := sarama.NewAsyncProducer(brokers, config) // 创建 Producer
		if err != nil {
			return nil
		}
		return producer
	}
	return provider
}

// 获得 Producer 实例
func (p *producerProvider) borrow() (producer sarama.AsyncProducer) {
	p.producersLock.Lock()
	defer p.producersLock.Unlock()

	if len(p.producers) == 0 {
		for {
			fmt.Printf("yyyyyy")
			producer = p.producerProvider()
			if producer != nil {
				return
			}
		}
	}

	index := len(p.producers) - 1
	producer = p.producers[index]
	p.producers = p.producers[:index]
	return
}

// 归还 Producer 实例
func (p *producerProvider) release(producer sarama.AsyncProducer) {
	p.producersLock.Lock()
	defer p.producersLock.Unlock()

	// If released producer is erroneous close it and don't return it to the producer pool.
	if producer.TxnStatus()&sarama.ProducerTxnFlagInError != 0 {
		// Try to close it
		_ = producer.Close()
		return
	}
	p.producers = append(p.producers, producer)
}

// 退出
func (p *producerProvider) clear() {
	p.producersLock.Lock()
	defer p.producersLock.Unlock()

	for _, producer := range p.producers {
		producer.Close()
	}
	p.producers = p.producers[:0]
}
