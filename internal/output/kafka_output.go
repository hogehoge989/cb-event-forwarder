package output

import (
	"errors"
	"fmt"
	"github.com/carbonblack/cb-event-forwarder/internal/encoder"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
	"os"
	"runtime"
	"sync"
	"syscall"
)

// Producer implements a High-level Apache Kafka Producer instance ZE 2018
// This allows Mocking producers w/o actual contact to kafka broker for testing purposes
type WrappedProducer interface {
	String() string

	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error

	Events() chan kafka.Event

	ProduceChannel() chan *kafka.Message

	Len() int

	Flush(timeoutMs int) int

	Close()

	GetMetadata(topic *string, allTopics bool, timeoutMs int) (*kafka.Metadata, error)

	QueryWatermarkOffsets(topic string, partition int32, timeoutMs int) (low, high int64, err error)

	OffsetsForTimes(times []kafka.TopicPartition, timeoutMs int) (offsets []kafka.TopicPartition, err error)
}

type KafkaOutput struct {
	brokers           string
	topicSuffix       string
	Producer          WrappedProducer
	deliveryChannel   chan kafka.Event
	droppedEventCount int64
	eventSentCount    int64
	Encoder encoder.Encoder
}

type KafkaStatistics struct {
	DroppedEventCount int64 `json:"dropped_event_count"`
	EventSentCount    int64 `json:"event_sent_count"`
}

func NewKafkaOutputFromCfg(cfg map[interface{}]interface{}, encoder encoder.Encoder) (KafkaOutput, error) {
	ko := KafkaOutput{}

	log.Infof("Trying to create kafka output with plugin section: %s", cfg)

	var configMap map[interface{}]interface{} = make(map[interface{}]interface{})

	if configm, ok := cfg["producer"].(map[interface{}]interface{}); ok {
		configMap = configm
	}

	if topicsuffix, ok := cfg["topicSuffix"]; ok {
		if topicsuffix, ok := topicsuffix.(string); ok {
			ko.topicSuffix = topicsuffix
		} else {
			ko.topicSuffix = ""
		}
	}

	kafkaConfigMap := kafka.ConfigMap{}

	for key, value := range configMap {
		ks := key.(string)
		switch value.(type) {
		case string:
			kafkaConfigMap[ks] = value.(string)
		case int:
			kafkaConfigMap[ks] = value.(int)
		case float32:
			kafkaConfigMap[ks] = value.(float32)
		case float64:
			kafkaConfigMap[ks] = value.(float64)
		case bool:
			kafkaConfigMap[ks] = value.(bool)
		default:
			kafkaConfigMap[ks] = fmt.Sprintf("%s", value)
		}
	}

	if brokers, ok := configMap["bootstrap.servers"]; ok {
		if brokers, ok := brokers.(string); ok {
			ko.brokers = brokers
		} else {
			ko.brokers = "localhost:9092"
		}
	}

	producer, err := kafka.NewProducer(&kafkaConfigMap)

	if err != nil {
		log.Infof("Failed to create producer: %s\n", err)
		return ko, err
	}

	log.Infof("Created Producer %v\n", producer)

	ko.Producer = producer

	ko.deliveryChannel = make(chan kafka.Event)

	ko.Encoder = encoder

	return ko, nil
}

func (o KafkaOutput) Go(messages <-chan map[string]interface{}, errorChan chan<- error, controlchan <-chan os.Signal, wg sync.WaitGroup) error {

	wg.Add(1)
	stoppubchan := make(chan struct{}, 1)
	var mypubwg sync.WaitGroup
	workersNum := runtime.NumCPU()
	for w := 0; w < workersNum; w++ {
		go func(wnum int) {
			mypubwg.Add(1)
			defer mypubwg.Done()
			shouldStop := false
			//topic := fmt.Sprintf("eventforwarder")
			for {
				select {
				case message := <-messages:
					//log.Info("GOT MESSAGE AT GO IN KAFKA")
					if encodedMsg, err := o.Encoder.Encode(message); err == nil {
						topic := message["type"].(string)
						//log.Infof("Msg in kafka output is %s",message)
						/*if topicString, ok := topic.(string); ok {
							topicString = strings.Replace(topicString, "ingress.event.", "", -1)
							topicString += o.topicSuffix
							o.output(topicString, encodedMsg)
						} else {
							log.Errorf("ERROR: Topic was not a string")
						}*/
						o.output(topic, encodedMsg)
					} /* else {
						//log.Errorf("ERROR IN KAFKA MESSAGE OUT : %v", err)
						errorChan <- err
					}*/
				case <-stoppubchan:
					//log.Info("stop request received ending publishing goroutine")
					shouldStop = true
				default:
					//log.Info("Timeout in kafka output worker go routine select")
					if shouldStop {
						//log.Info("Stopping kafka output worker")
						return
					} /*else {
						log.Info("Not stopping kafka output worker")
					}*/
				}
			}
		}(w)
	}

	go func() {
		defer wg.Done()
		defer o.Producer.Close()
		for {
			select {
			/*
			case e := <-o.deliveryChannel:
				log.Info("GOT MESSAGE FROM DELIVERY CHANNEL")
				m := e.(*kafka.Message)
				if m.TopicPartition.Error != nil {
					log.Infof("Delivery failed: %v\n", m.TopicPartition.Error)
					atomic.AddInt64(&o.droppedEventCount, 1)
					errorChan <- m.TopicPartition.Error
				} else {
					log.Infof("Delivered message to topic %s [%d] at offset %v\n",
						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
					atomic.AddInt64(&o.eventSentCount, 1)
				}
			*/
			case cmsg := <-controlchan:
				switch cmsg {
				case syscall.SIGTERM, syscall.SIGINT:
					// handle exit gracefully
					log.Info("Received SIGTERM. Exiting")
					o.Producer.Flush(5)
					stoppubchan <- struct{}{}
					mypubwg.Wait()
					return
				}
			}
		}
	}()

	return nil
}

func (o KafkaOutput) Statistics() interface{} {
	return KafkaStatistics{DroppedEventCount: o.droppedEventCount, EventSentCount: o.eventSentCount}
}

func (o KafkaOutput) String() string {
	return fmt.Sprintf("Brokers %s", o.brokers)
}

func (o KafkaOutput) Key() string {
	return fmt.Sprintf("brokers:%s", o.brokers)
}

func (o KafkaOutput) output(topic string, m string) {

	err := errors.New("")
	//IF we hit the kernel buffer limit, flush and keep going
	//log.Infof("TRING TO PRODUCE TO %s topic ", topic)
	for err != nil {
		err = o.Producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(m),
		}, nil)
		if err != nil {
			log.Errorf("%v ",err)
			//log.Errorf("got error at production...flushing")
			o.Producer.Flush(1)
		}
	}
	//log.Infof("Send out production ok")
}

func GetOutputHandler(cfg map[interface{}]interface{}, encoder encoder.Encoder) (OutputHandler, error) {
	ko, err := NewKafkaOutputFromCfg(cfg, encoder)
	return &ko, err
}
