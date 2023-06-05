package main

// SIGUSR1 toggle the pause/resume consumption
import (
	"github.com/Shopify/sarama"
	"log"
)

// Sarama configuration options
var (
	//brokers  = "127.0.0.1:9092"
	//version  = "3.2.3"
	group    = "test-"
	topics   = "saram"
	assignor = "sticky"
	oldest   = true
	//verbose  = false
)

func init() {
	//flag.StringVar(&brokers, "brokers", "", "Kafka bootstrap brokers to connect to, as a comma separated list")
	//flag.StringVar(&group, "group", "", "Kafka consumer group definition")
	//flag.StringVar(&version, "version", "2.1.1", "Kafka cluster version")
	//flag.StringVar(&topics, "topics", "", "Kafka topics to be consumed, as a comma separated list")
	//flag.StringVar(&assignor, "assignor", "range", "Consumer group partition assignment strategy (range, roundrobin, sticky)")
	//flag.BoolVar(&oldest, "oldest", true, "Kafka consumer consume initial offset from oldest")
	//flag.BoolVar(&verbose, "verbose", false, "Sarama logging")
	//flag.Parse()

	if len(brokers) == 0 {
		panic("no Kafka bootstrap brokers defined, please set the -brokers flag")
	}

	if len(topics) == 0 {
		panic("no topics given to be consumed, please set the -topics flag")
	}

	if len(group) == 0 {
		panic("no Kafka consumer group defined, please set the -group flag")
	}
}

func toggleConsumptionFlow(client sarama.ConsumerGroup, isPaused *bool) {
	if *isPaused {
		client.ResumeAll()
		log.Println("Resuming consumption")
	} else {
		client.PauseAll()
		log.Println("Pausing consumption")
	}

	*isPaused = !*isPaused
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready chan bool
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/main/consumer_group.go#L27-L29
	for {
		select {
		case message := <-claim.Messages():
			log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s, Offset = %d", string(message.Value), message.Timestamp, message.Topic, message.Offset)
			session.MarkMessage(message, "kkkkkkkkk")

		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/Shopify/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}
