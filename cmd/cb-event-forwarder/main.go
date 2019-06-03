package main

import (
	"expvar"
	"flag"
	"github.com/carbonblack/cb-event-forwarder/internal/cbeventforwarder"
	conf "github.com/carbonblack/cb-event-forwarder/internal/config"
	"github.com/carbonblack/cb-event-forwarder/internal/consumer"
	log "github.com/sirupsen/logrus"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

import _ "net/http/pprof"

var (
	checkConfiguration = flag.Bool("check", false, "Check the configuration file and exit")
	debug              = flag.Bool("debug", false, "Enable debugging mode")
	httpserverport     = flag.Int("httpserverport", 33706, "Enter port for debugging")
)

var version = "3.6.0 BETA"

/*
 * Initializations
 */

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
	flag.Parse()
	log.SetOutput(NewBufwriter(10000))
}

func main() {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	exportedVersion := expvar.NewString("version")
	log.Infof("Debug is %t", *debug)
	if *debug {
		exportedVersion.Set(version + " (debugging on)")
		log.Debugf("*** Debugging enabled: messages may be sent via http://%s:%d/debug/sendmessage/<cbefinputname> ***",
			hostname, *httpserverport)
		log.SetLevel(log.DebugLevel)
	} else {
		exportedVersion.Set(version)
		log.SetLevel(log.InfoLevel)
	}
	expvar.Publish("debug", expvar.Func(func() interface{} {
		return *debug
	}))

	dirs := [...]string{
		"/usr/share/cb/integrations/event-forwarder/content",
		"./static",
	}

	for _, dirname := range dirs {
		finfo, err := os.Stat(dirname)
		if err == nil && finfo.IsDir() {
			http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir(dirname))))
			http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, "/static/", 301)
			})
			log.Infof("Diagnostics available via HTTP at http://%s:%d/", hostname, *httpserverport)
			break
		}
	}

	configLocation := "/etc/cb/integrations/event-forwarder/cb-event-forwarder.conf"
	if flag.NArg() > 0 {
		configLocation = flag.Arg(0)
	}

	config, err := conf.ParseConfig(configLocation)
	if err != nil {
		log.Fatal(err)
	} else {
		log.Debugf("Parsed configuration ok...")
	}

	cbef := cbeventforwarder.GetCbEventForwarderFromCfg(config, consumer.StreadwayAMQPDialer{})

	log.Debugf("Created cb-event-forwarder ok")

	addrs, err := net.InterfaceAddrs()

	if err != nil {
		log.Fatal("Could not get IP addresses")
	}

	log.Infof("cb-event-forwarder %s starting ", version)

	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			log.Infof("Interface address %s", ipnet.IP.String())
		}
	}

	if *checkConfiguration {
		if err := cbef.StartOutput(); err != nil {
			log.Fatal(err)
		}
		os.Exit(0)
	}


	if *debug {

		 //TODO FIX DEBUG EXPVAR HANDLING HERE
		 /*http.HandleFunc(fmt.Sprintf("/debug/sendmessage"), func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "POST" {
				msg := make([]byte, r.ContentLength)
				_, err := r.Body.Read(msg)
				var parsedMsg map[string]interface{}
				err = json.Unmarshal(msg, &parsedMsg)
				if err != nil {
					errMsg, _ := json.Marshal(map[string]string{"status": "error", "error": err.Error()})
					_, _ = w.Write(errMsg)
					return
				}

				err = cbef.OutputMessage(parsedMsg)
				if err != nil {
					errMsg, _ := json.Marshal(map[string]string{"status": "error", "error": err.Error()})
					_, _ = w.Write(errMsg)
					return
				}
				log.Errorf("Sent test message: %s\n", string(msg))
			} else {
				err = cbef.OutputMessage(map[string]interface{}{
					"type":    "debug.message",
					"message": fmt.Sprintf("Debugging test message sent at %s", time.Now().String()),
				})
				if err != nil {
					errMsg, _ := json.Marshal(map[string]string{"status": "error", "error": err.Error()})
					_, _ = w.Write(errMsg)
					return
				}
				log.Info("Sent test debugging message")
			}

			errMsg, _ := json.Marshal(map[string]string{"status": "success"})
			_, _ = w.Write(errMsg)
		}) */
	}

	//go http.ListenAndServe(fmt.Sprintf(":%d", *httpserverport), nil)

	sigs := make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	cbef.Go(sigs)

}
