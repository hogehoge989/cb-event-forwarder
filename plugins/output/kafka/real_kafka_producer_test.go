package main

/**
* Copyright 2018 Carbon Black
*
*


CARBON BLACK 2018 - Zachary Estep - Using this code as the basis for a producer interface that is mockable
*/

import (
	"github.com/carbonblack/cb-event-forwarder/internal/encoder"
	"github.com/carbonblack/cb-event-forwarder/internal/jsonmessageprocessor"
	"github.com/carbonblack/cb-event-forwarder/gotests"
	"github.com/carbonblack/cb-event-forwarder/internal/cbeventforwarder"
	"github.com/carbonblack/cb-event-forwarder/internal/output"
	"github.com/carbonblack/cb-event-forwarder/internal/pbmessageprocessor"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"syscall"
	"testing"
)

type bufwriter chan []byte

func (bw bufwriter) Write(p []byte) (int, error) {
    bw <- p
    return len(p), nil
}
func NewBufwriter(n int) bufwriter {
    w := make(bufwriter, n)
    go func() {
        for p := range w {
            os.Stdout.Write(p)
        }
    }()
    return w
}

func init() {
	log.SetOutput(NewBufwriter(10000))
}

//var jsmp jsonmessageprocessor.JsonMessageProcessor = jsonmessageprocessor.JsonMessageProcessor{}
var eventMap map[string]interface{} = map[string]interface{}{
	"watchlist.#":                  true,
	"feed.#":                       true,
	"alert.#":                      true,
	"ingress.event.process":        true,
	"ingress.event.procstart":      true,
	"ingress.event.netconn":        true,
	"ingress.event.procend":        true,
	"ingress.event.childproc":      true,
	"ingress.event.moduleload":     true,
	"ingress.event.module":         true,
	"ingress.event.filemod":        true,
	"ingress.event.regmod":         true,
	"ingress.event.tamper":         true,
	"ingress.event.crossprocopen":  true,
	"ingress.event.remotethread":   true,
	"ingress.event.processblock":   true,
	"ingress.event.emetmitigation": true,
	"binaryinfo.#":                 true,
	"binarystore.#":                true,
	"events.partition.#":           true,
}
var pbmp pbmessageprocessor.PbMessageProcessor = pbmessageprocessor.PbMessageProcessor{EventMap: eventMap}

type outputMessageFunc func([]map[string]interface{}) (string, error)

/* func TestKafkaProducer(t *testing.T) {
	Producer, _ := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})

	topic := "test"

	data := make([] byte, 1000)

	for true {
		var err error = errors.New("Didn't produce yet")
		for err != nil {
			err = Producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          data,
			}, nil)
			if err != nil {
				log.Debugf("got error at production...flushing")
				Producer.Flush(1)
			}
		}
	}
} */

func TestKafkaOutput(t *testing.T) {
	// create an instance of our test object

	t.Logf("Starting kafkaoutput test")
	outputDir := "../../../test_output/real_output_kafka"
	os.MkdirAll(outputDir, 0755)

	/*outputFile, err := os.Create(path.Join(outputDir, "/kafkaoutput")) // For read access.
	if err != nil {
		t.Errorf("Coudln't open httpoutput file %v", err)
		t.FailNow()
		return
	}
	mockProducer := new(MockedProducer)
	mockProducer.outfile = outputFile*/
	mockProducer, _ := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	testEncoder := encoder.NewJSONEncoder()
	var outputHandler output.OutputHandler = &KafkaOutput{Producer: mockProducer, Encoder: &testEncoder, deliveryChannel: make(chan kafka.Event)}
	processTestEventsWithRealHandler(t, outputDir, jsonmessageprocessor.MarshalJSON, outputHandler)

}

func TestKafkaOutputReal(t *testing.T) {
	t.Logf("Starting kafkaoutput test")
	outputDir := "../../../test_output/real_output_kafka_real"
	os.MkdirAll(outputDir, 0755)
	/*
		outputFile, err := os.Create(path.Join(outputDir, "/kafkaoutput")) // For read access.
		if err != nil {
			t.Errorf("Coudln't open httpoutput file %v", err)
			t.FailNow()
			return
		}

		mockProducer := new(MockedProducer)
		mockProducer.outfile = outputFile
	*/

	conf := map[string]interface{}{"debug": true,
		"use_time_float": true,
		"input":          map[interface{}]interface{}{"cb_server_url": "https://cbresponseserver", "bind_raw_exchange": true, "rabbit_mq_password": "lol", "rabbit_mq_port": 5672},
		"output":         map[interface{}]interface{}{"plugin": map[interface{}]interface{}{"path": ".", "name": "kafka_output", "config": map[interface{}]interface{}{"producer": map[interface{}]interface{}{"bootstrap.servers": "localhost:9092"}}, "format": map[interface{}]interface{}{"type": "json"}}},
	}

	processTestEventsWithRealForwarder(t, conf, outputDir, nil, nil)
}

func processTestEventsWithRealHandler(t *testing.T, outputDir string, outputFunc outputMessageFunc, oh output.OutputHandler) {
	t.Logf("Tring to perform test with %v %s", oh, oh)

	/*formats := [...]struct {
		formatType string
		process    func(string, []byte) ([]map[string]interface{}, error)
	}{{"json", jsmp.ProcessJSON}, {"protobuf", pbmp.ProcessProtobuf}}
	*/

	formats := [...]struct {
		formatType string
		process    func(string, []byte) ([]map[string]interface{}, error)
	}{{"protobuf", pbmp.ProcessProtobuf}}

	messages := make(chan map[string]interface{})
	errors := make(chan error)
	controlchan := make(chan os.Signal, 5)
	var wg sync.WaitGroup
	wg.Add(1)

	oh.Go(messages, errors, controlchan, wg)

	for _, format := range formats {
		pathname := path.Join("../../../test/raw_data", format.formatType)
		fp, err := os.Open(pathname)
		if err != nil {
			t.Logf("Could not open %s", pathname)
			t.FailNow()
		}

		infos, err := fp.Readdir(0)
		if err != nil {
			t.Logf("Could not enumerate directory %s", pathname)
			t.FailNow()
		}

		fp.Close()

		for _, info := range infos {
			if !info.IsDir() {
				continue
			}

			routingKey := info.Name()
			os.MkdirAll(outputDir, 0755)

			// process all files inside this directory
			routingDir := path.Join(pathname, info.Name())
			fp, err := os.Open(routingDir)
			if err != nil {
				t.Logf("Could not open directory %s", routingDir)
				t.FailNow()
			}

			files, err := fp.Readdir(0)
			if err != nil {
				t.Errorf("Could not enumerate directory %s; continuing", routingDir)
				continue
			}

			fp.Close()
			for _, fn := range files {
				if fn.IsDir() {
					continue
				}

				fp, err := os.Open(path.Join(routingDir, fn.Name()))
				if err != nil {
					t.Errorf("Could not open %s for reading", path.Join(routingDir, fn.Name()))
					continue
				}
				b, err := ioutil.ReadAll(fp)
				if err != nil {
					t.Errorf("Could not read %s", path.Join(routingDir, fn.Name()))
					continue
				}

				fp.Close()

				msgs, err := format.process(routingKey, b)
				if err != nil {
					t.Errorf("Error processing %s: %s", path.Join(routingDir, fn.Name()), err)
					continue
				}
				if len(msgs[0]) == 0 {
					t.Errorf("got zero messages out of: %s/%s", routingDir, fn.Name())
					continue
				}
				for _, msg := range msgs {
					messages <- msg
				}
				_, err = outputFunc(msgs)
				if err != nil {
					t.Errorf("Error serializing %s: %s", path.Join(routingDir, fn.Name()), err)
					continue
				}
			}
		}
	}
	t.Logf("Done with test for %s ", oh)
	controlchan <- syscall.SIGTERM
}

func processTestEventsWithRealForwarder(t *testing.T, conf map[string]interface{}, outputDir string, backgroundfunc *func(), shutdown *func()) {

	t.Logf("Tring to preform test")

	if backgroundfunc != nil {
		t.Logf("Executing background func")
		go (*backgroundfunc)()
	}

	t.Logf("Background running...continue to test...")

	formats := []string{"unzip"}

	sigs := make(chan os.Signal)

	mockConn := tests.MockAMQPConnection{AMQPURL: "amqp://cb:lol@localhost:5672"}

	mockChan, _ := mockConn.Channel()

	mockDialer := tests.MockAMQPDialer{Connection: mockConn}

	cbef := cbeventforwarder.GetCbEventForwarderFromCfg(conf, mockDialer)

	go cbef.Go(sigs, nil)

		for _, format := range formats {

			t.Logf("Trying to use format %s", format)

			pathname := path.Join("../../../test/raw_data", format)
			fp, err := os.Open(pathname)

			if err != nil {
				t.Logf("Could not open %s", pathname)
				t.FailNow()
			}

			infos, err := fp.Readdir(0)
			if err != nil {
				t.Logf("Could not enumerate directory %s", pathname)
				t.FailNow()
			}

			fp.Close()

			t.Logf("Enumerated directory for %s", format)

			for _, info := range infos {

				t.Logf("Info = %s", info)

				if !info.IsDir() {
					t.Logf("SKIPPING!")
					continue
				}

				routingKey := info.Name()
				if format == "zip" || format == "unzip" {
					routingKey = ""
				}
				os.MkdirAll(outputDir, 0755)

				// process all files inside this directory
				routingDir := path.Join(pathname, info.Name())
				fp, err := os.Open(routingDir)
				if err != nil {
					t.Logf("Could not open directory %s", routingDir)
					t.FailNow()
				}

				files, err := fp.Readdir(0)
				if err != nil {
					t.Errorf("Could not enumerate directory %s; continuing", routingDir)
					t.FailNow()
					continue
				}

				fp.Close()

				for _, fn := range files {
					log.Infof("Trying to load file %s", fn)
					if fn.IsDir() {
						continue
					}

					fp, err := os.Open(path.Join(routingDir, fn.Name()))
					if err != nil {
						t.Errorf("Could not open %s for reading", path.Join(routingDir, fn.Name()))
						continue
					}
					b, err := ioutil.ReadAll(fp)
					if err != nil {
						t.Errorf("Could not read %s", path.Join(routingDir, fn.Name()))
						continue
					}

					fp.Close()

					exchange := "api.events"
					contentType := "application/json"
					if format == "json" {
						exchange = "api.events"
					} else if format == "protobuf" {
						exchange = "api.rawsensordata"
						contentType = "application/protobuf"
					} else if format == "zip" {
						exchange = "api.rawsensordata"
						contentType = "application/zip"
					} else if format == "unzip" {
						exchange = "api.rawsensordata"
						contentType = "application/protobuf"
					}
					for true {
						err = mockChan.Publish(
							exchange, // publish to an exchange
							routingKey, // routing to 0 or more queues
							false, // mandatory
							false, // immediate
							amqp.Publishing{
								Headers:         amqp.Table{},
								ContentType:     contentType,
								ContentEncoding: "",
								Body:            b,
								DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
								Priority:        0, // 0-
							},
						)
						/*if err != nil {
							t.Errorf("Failed to publish %s %s: %s", exchange, routingKey, err)
						} else {
							t.Logf("PUBLISHED A MESSAGE!")
						}*/
					}
				}
			}
		}

	t.Logf("Done with test")

	if shutdown != nil {
		(*shutdown)()
	}

	sigs <- syscall.SIGTERM
}
