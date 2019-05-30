package main

/**
* Copyright 2018 Carbon Black
*
*


CARBON BLACK 2018 - Zachary Estep - Using this code as the basis for a producer interface that is mockable
*/

import (
	"github.com/carbonblack/cb-event-forwarder/gotests"
	"github.com/carbonblack/cb-event-forwarder/internal/cbeventforwarder"
	"github.com/carbonblack/cb-event-forwarder/internal/consumer"
	"github.com/carbonblack/cb-event-forwarder/internal/encoder"
	"github.com/carbonblack/cb-event-forwarder/internal/jsonmessageprocessor"
	"github.com/carbonblack/cb-event-forwarder/internal/output"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/streadway/amqp"
	"io/ioutil"
	"os"
	"path"
	"syscall"
	"testing"
)

func TestKafkaOutputMock(t *testing.T) {
	// create an instance of our test object

	t.Logf("Starting kafkaoutput test")
	outputDir := "../../../test_output/real_output_kafka_mock"
	os.MkdirAll(outputDir, 0755)

	outputFile, err := os.Create(path.Join(outputDir, "/kafkaoutput")) // For read access.
	if err != nil {
		t.Errorf("Coudln't open httpoutput file %v", err)
		t.FailNow()
		return
	}
	mockProducer := new(MockedProducer)
	mockProducer.outfile = outputFile
	testEncoder := encoder.NewJSONEncoder()
	var outputHandler output.OutputHandler = &KafkaOutput{Producer: mockProducer, Encoder: &testEncoder, deliveryChannel: make(chan kafka.Event)}
	processTestEventsWithRealHandler(t, outputDir, jsonmessageprocessor.MarshalJSON, outputHandler)

}

func BenchmarkKafkaOutput(b *testing.B) {
	fn := path.Join("../../../test/stress_rabbit/zipbundles/1")
	fp, _ := os.Open(fn)
	d, _ := ioutil.ReadAll(fp)
	fp.Close()
	exchange := "api.rawsensordata"
	contentType := "application/zip"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := mockChan.Publish(
			exchange, // publish to an exchange
			"zip",    // routing to 0 or more queues
			false,    // mandatory
			false,    // immediate
			amqp.Publishing{
				Headers:         amqp.Table{},
				ContentType:     contentType,
				ContentEncoding: "",
				Body:            d,
				DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
				Priority:        0,              // 0-
			},
		)
		b.StopTimer()
		if err != nil {
			b.Errorf("Failed to publish %s : %s", exchange, err)
		} /* else {
			b.Logf("PUBLISHED A MESSAGE!")
		}*/
		b.StartTimer()
	}
	b.Logf("DONE BENCHMARKING!")
}

var mockChan consumer.AMQPChannel
var mockConn tests.MockAMQPConnection
var cbef cbeventforwarder.CbEventForwarder
var sigs chan os.Signal = make(chan os.Signal)

func TestMain(m *testing.M) {
	mockConn = tests.MockAMQPConnection{AMQPURL: "amqp://cb:lol@localhost:5672"}

	mockChan, _ = mockConn.Channel()

	mockDialer := tests.MockAMQPDialer{Connection: mockConn}

	conf := map[string]interface{}{"debug": true,
		"use_time_float": true,
		"input":          map[interface{}]interface{}{"cb_server_url": "https://cbresponseserver", "bind_raw_exchange": true, "rabbit_mq_password": "lol", "rabbit_mq_port": 5672},
		"output":         map[interface{}]interface{}{"plugin": map[interface{}]interface{}{"path": ".", "name": "kafka_output", "config": map[interface{}]interface{}{"producer": map[interface{}]interface{}{"bootstrap.servers": "localhost:9092"}}, "format": map[interface{}]interface{}{"type": "json"}}},
	}

	cbef = cbeventforwarder.GetCbEventForwarderFromCfg(conf, mockDialer)

	outputFile, err := os.Create(path.Join("../../../test_output/real_output_kafka_real", "/kafkaoutputmocked")) // For read access.

	if err != nil {
		//m.Errorf("Coudln't open httpoutput file %v", err)
		//m.FailNow()
		return
	}

	mockProducer := new(MockedProducer)
	mockProducer.outfile = outputFile

	testEncoder := encoder.NewJSONEncoder()

	var outputHandler output.OutputHandler = &KafkaOutput{Producer: mockProducer, Encoder: &testEncoder, deliveryChannel: make(chan kafka.Event)}

	cbef.Output = outputHandler

	go cbef.Go(sigs, nil)

	runresults := m.Run()

	sigs <- syscall.SIGTERM

	os.Exit(runresults)

}
