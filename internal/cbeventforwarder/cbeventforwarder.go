package cbeventforwarder

import (
	"encoding/json"
	"expvar"
	"fmt"
	"github.com/carbonblack/cb-event-forwarder/internal/cbapi"
	"github.com/carbonblack/cb-event-forwarder/internal/consumer"
	"github.com/carbonblack/cb-event-forwarder/internal/filter"
	"github.com/carbonblack/cb-event-forwarder/internal/jsonmessageprocessor"
	"github.com/carbonblack/cb-event-forwarder/internal/output"
	"github.com/carbonblack/cb-event-forwarder/internal/pbmessageprocessor"
	log "github.com/sirupsen/logrus"
	"os"
	"sync"
	"syscall"
	"time"
)

type Status struct {
	OutputEventCount   *expvar.Int
	FilteredEventCount *expvar.Int
	ErrorCount         *expvar.Int
	IsConnected        bool
	LastConnectTime    time.Time
	StartTime          time.Time
	LastConnectError   string
	ErrorTime          time.Time
	sync.RWMutex
}

type CbEventForwarder struct {
	Config            map[string]interface{}
	AddToOutput       map[string]interface{}
	RemoveFromOutput  []string
	OutputErrors      chan error
	Result    	  chan map[string] interface{}
	Consumer         *consumer.Consumer
	Output           output.OutputHandler
	Filter            *filter.Filter
	Controlchan      chan os.Signal ///controls output
	Status            Status
	DebugFlag         bool
	DebugStore        string
	Name              string
	ConsumerWaitGroup sync.WaitGroup
	OutputWaitGroup   sync.WaitGroup
}

/*
 * worker
 */

func (cbef *CbEventForwarder) OutputMessage(msg map[string]interface{}) error {
	var err error
	//
	// Marshal result into the correct output format
	//
	//msg["cb_server"] = cbef.CbServerName

	// Add key=value pairs that has been configured to be added
	for key, val := range cbef.AddToOutput {
		msg[key] = val
	}

	// Remove keys that have been configured to be removed
	for _, v := range cbef.RemoveFromOutput {
		delete(msg, v)
	}

	//Apply Event Filter if specified
	keepEvent := true
	if cbef.Filter != nil {
		keepEvent = cbef.Filter.FilterEvent(msg)
	}

	if keepEvent {
		if len(msg) > 0 && err == nil {
			cbef.Result <- msg
			cbef.Status.OutputEventCount.Add(1)
		} else if err != nil {
			return err
		}
	} else { //EventDropped due to filter
		cbef.Status.FilteredEventCount.Add(1)
		log.Debugf("Filtered Event %d", cbef.Status.FilteredEventCount)
	}
	return nil
}

func (cbef *CbEventForwarder) InputFileProcessingLoop(inputFile string) <-chan error {
	errChan := make(chan error)
	go func() {
		log.Debugf("Opening input file : %s", inputFile)
		_, deliveries, err := consumer.NewFileConsumer(inputFile)
		if err != nil {
			cbef.Status.LastConnectError = err.Error()
			cbef.Status.ErrorTime = time.Now()
			errChan <- err
		}
		for delivery := range deliveries {
			log.Debug("Trying to deliver log message %s", delivery)
			msgMap := make(map[string]interface{})
			err := json.Unmarshal([]byte(delivery), &msgMap)
			if err != nil {
				cbef.Status.LastConnectError = err.Error()
				cbef.Status.ErrorTime = time.Now()
				errChan <- err
			}
			cbef.OutputMessage(msgMap)
		}
	}()
	return errChan
}

func (cbef *CbEventForwarder) startExpvarPublish() {
	expvar.Publish(fmt.Sprintf("connection_status_%s", cbef.Name),
		expvar.Func(func() interface{} {
			res := make(map[string]interface{}, 0)
			res["last_connect_time"] = cbef.Status.LastConnectTime
			res["last_error_text"] = cbef.Status.LastConnectError
			res["last_error_time"] = cbef.Status.ErrorTime
			if cbef.Status.IsConnected {
				res["connected"] = true
				res["uptime"] = time.Now().Sub(cbef.Status.LastConnectTime).Seconds()
			} else {
				res["connected"] = false
				res["uptime"] = 0.0
			}

			return res
		}))
}

//Terminate consumers

func (cbef *CbEventForwarder) TerminateConsumer() {
	log.Debugf("%s trying to stop consumer...", cbef.Name)
	log.Debugf("Consumer = %s stopchan = %s", cbef.Consumer.CbServerName, &cbef.Consumer.Stopchan)
	cbef.Consumer.Stopchan <- struct{}{}
}

// launch the amqp consumer goroutines
func (cbef *CbEventForwarder) LaunchConsumer() {
	log.Infof("%s launching amqp consumer %s", cbef.Name, cbef.Consumer.CbServerName)
	cbef.Consumer.Consume()
}

func (cbef *CbEventForwarder) StartOutput() error {
	log.Infof("Trying to start output %s", cbef.Output)
	expvar.Publish(fmt.Sprint("output_status_%d", cbef.Output), expvar.Func(func() interface{} {
		ret := make(map[string]interface{})
		ret[cbef.Output.Key()] = cbef.Output.Statistics()
		ret["type"] = cbef.Output.String()
		return ret
	}))
	if err := cbef.Output.Go(cbef.Result, cbef.OutputErrors, cbef.Controlchan, cbef.OutputWaitGroup); err != nil {
		return err
	}
	log.Infof("Successfully Initialized output: %s ", cbef.Output.String())

	go func() {
		select {
		case outputError := <-cbef.OutputErrors:
			log.Errorf("ERROR during output: %s", outputError.Error())
		}
	}()
	return nil
}

func conversionFailure(i interface{}) {
	switch t := i.(type) {
	default:
		log.Errorf("Failed to convert %T %s", t, t)
	}
}

func GetCbEventForwarderFromCfg(config map[string]interface{}, dialer consumer.AMQPDialer) CbEventForwarder {

	debugFlag := false
	if t, ok := config["debug"]; ok {
		debugFlag = t.(bool)
	}

	if debugFlag {
		log.SetLevel(log.DebugLevel)
		log.Debugf("Set log level to debug")
	}

	debugStore := "/tmp"
	if t, ok := config["debug_store"]; ok {
		debugStore = t.(string)
	}

	useTimeFloat := true
	if t, ok := config["use_time_float"]; ok {
		useTimeFloat = t.(bool)
	}

	log.Debugf("Trying to load event forwarder from config: %s", config)

	outputE := make(chan error)

	var myfilter *filter.Filter
	var err error = nil

	if t, ok := config["filter"]; ok {
		myfilter = filter.GetFilterFromCfg(t.(map[interface{}]interface{}))
		log.Debugf("Filter created OK")
	}


	cbServerName := "cbresponse"
	consumerconfig := make(map[interface{}]interface{})
	if t, ok := config["input"]; ok {
		consumerconfig = t.(map[interface{}]interface{})
		if t, ok := consumerconfig["cb_server_name"]; ok {
			cbServerName = t.(string)
		}
	} else {
		log.Panicf("No input section specified in conf file!")
	}

	outputconfig := make(map[interface{}] interface{}, 0)
	if t, ok := config["output"]; ok {
		outputconfig = t.(map[interface{}]interface{})
	}

	res := make(chan map[string] interface{})
	outputcontrolchannel := make(chan os.Signal)

	output, err := output.GetOutputFromCfg(outputconfig)
	if err != nil {
		log.Panicf("ERROR PROCESSING OUTPUT CONFIGURATIONS %v", err)
	} else {
		log.Infof("Detected ouput...%s",output.Key())
	}

	addToOutput := make(map[string]interface{})
	if toadd, ok := config["addToOutput"]; ok {
		addToOutputI := toadd.(map[interface{}]interface{})
		for k, v := range addToOutputI {
			addToOutput[k.(string)] = v
		}
	}

	removeFromOutput := make([]string, 0)
	if remove, ok := config["removeFromOutput"]; ok {
		rai := remove.([]interface{})
		removeFromOutput = make([]string, len(rai))
		for i, r := range rai {
			removeFromOutput[i] = r.(string)
		}
	}

	cbef := CbEventForwarder{Controlchan: outputcontrolchannel, AddToOutput: addToOutput, RemoveFromOutput: removeFromOutput, Name: "cb-event-forwarder", Output: output, Filter: myfilter, OutputErrors: outputE, Result : res, Config: config, Status: Status{ErrorCount: expvar.NewInt("cbef_error_count"), FilteredEventCount: expvar.NewInt("filtered_event_count"), OutputEventCount: expvar.NewInt("output_event_count")}}
	//cbef.OutputWaitGroup.Add(1)
	log.Infof("Configuring Cb Event Forwarder %s", cbef.Name)
	log.Infof("Configured to remove keys: %s", cbef.RemoveFromOutput)
	log.Infof("Configured to add k-vs to output: %s", cbef.AddToOutput)

	log.Debugf("CONFIG: %s , %s ", cbServerName, consumerconfig)

	cbServerURL := ""
	if t, ok := consumerconfig["cb_server_url"]; ok {
		cbServerURL = t.(string)
	}

	var cbapihandler *cbapi.CbAPIHandler = nil

	if postprocess, ok := consumerconfig["post_processing"]; ok {
		if ppmap, ok := postprocess.(map[interface{}]interface{}); ok {
			cbapihandler_temp, err := cbapi.CbAPIHandlerFromCfg(ppmap, cbServerURL)
			if err != nil {
				log.Panicf("Error getting cbapihandler from configuration: %v", err)
			} else {
				cbapihandler = cbapihandler_temp
			}
		} else {
			log.Panicf("Error getting cbapihandler from configuration: %v", err)
		}
	}

	myjsmp := jsonmessageprocessor.JsonMessageProcessor{DebugFlag: debugFlag, DebugStore: debugStore, CbAPI: cbapihandler, CbServerURL: cbServerURL, UseTimeFloat: useTimeFloat}
	mypbmp := pbmessageprocessor.PbMessageProcessor{DebugFlag: debugFlag, DebugStore: debugStore, CbServerURL: cbServerURL, UseTimeFloat: useTimeFloat}

	c, err := consumer.NewConsumerFromConf(cbef.OutputMessage, cbServerName, cbServerName, consumerconfig, debugFlag, debugStore, cbef.ConsumerWaitGroup, dialer)
	if err != nil {
		log.Panicf("Error constructing consumer from configuration: %v", err)
	}

	eventMap := make(map[string]interface{})
	for _, e := range c.RoutingKeys {
		eventMap[e] = true
	}

	mypbmp.EventMap = eventMap
	c.Jsmp = myjsmp
	c.Pbmp = mypbmp
	cbef.Consumer = c

	log.Infof("Configuration success")

	return cbef
}

// The event forwarder GO
//  The sigs channel should be hooked up to system or similar
// This controls the event forwarder, and should be used to cause it to gracefully exit/clear output buffers
// The optional (pass null to opt-out) inputFile argument (usually from commandline) explicity lists an input jsonfile to grab
// and use for (additional) input
func (cbef *CbEventForwarder) Go(sigs chan os.Signal, inputFile *string) {
	cbef.startExpvarPublish()

	if err := cbef.StartOutput(); err != nil {
		log.Fatalf("Could not startOutput: %v", err)
	}

	cbef.LaunchConsumer()

	if inputFile != nil {
		go func() {
			errChan := cbef.InputFileProcessingLoop(*inputFile)
			for {
				select {
				case err := <-errChan:
					log.Errorf("Input file processing loop error: %v", err)
				}
			}
		}()
	}

	for {
		log.Info("cb-event forwarder running...")
		select {
		case sig := <-sigs:
			log.Debugf("Signal handler got Signal %s ", sig)
			switch sig {
			case syscall.SIGTERM, syscall.SIGINT:
				//termiante consumers, then outputs
				log.Info("cb-event-forwarder beginning shutdown")
				cbef.TerminateConsumer()
				//should also be a method something like 'stopOutputs(sig)'
				log.Debugf("Signal %s to output control channel", sig)
				cbef.Controlchan <- sig
				log.Debugf("cb-event-forwarder awaiting consumer exit...")
				cbef.ConsumerWaitGroup.Wait()
				log.Debugf("cb-event-forwarder awaiting output exit...")
				cbef.OutputWaitGroup.Wait()
				log.Debugf("Consumer workers & output finished gracefully - cb-event-forwarder service exiting")
				log.Debugf("cb-event-forwarder graceful shutdown done...")
				return
			case syscall.SIGHUP: //propgate signals down to the outputs (HUP)
				log.Debugf("Propogating  HUP signal to output control channel ")
				cbef.Controlchan <- sig
			}
		}
	}
}
