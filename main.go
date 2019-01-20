package main

import (
	"clustercode-api-gateway/schema"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/micro/go-config"
	"github.com/micro/go-config/source/env"
	"github.com/micro/go-config/source/file"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"net/http"
	"os"
	"strconv"
)

func main() {
	LoadConfig()
	ConfigureLogging()
	ConfigureMessaging()
	Initialize()
	//healthCheckQueue = messaging.OpenHealthCheckServer()

	addr := config.Get("http", "addr").String(":8080")
	r := mux.NewRouter()

	if config.Get("prometheus", "enabled").Bool(true) {
		r.Handle("/metrics", promhttp.Handler())
	}

	r.HandleFunc("/", handleRoot)
	r.HandleFunc("/schema/v{version:\\d+}/clustercode.xsd", handleSchema)
	http.Handle("/", r)

	log.WithField("port", addr).Info("Starting http server")
	err := http.ListenAndServe(addr, nil)
	log.Error(err)
}

func ConfigureMessaging() {
	validator = schema.NewXmlValidator(config.
		Get("api", "schema", "latest").
		String("schema/clustercode_v1.xsd"))
}

func handleRoot(writer http.ResponseWriter, request *http.Request) {
	_, err := fmt.Fprintf(writer, "This page is intentionally left blank. You might want to check /health")
	if err != nil {
		log.Warn(err)
	}
}

func handleSchema(writer http.ResponseWriter, request *http.Request) {
	version, _ := strconv.Atoi(mux.Vars(request)["version"])
	pattern := config.Get("api", "schema", "filepattern").String("schema/clustercode_v%d.xsd")
	path := fmt.Sprintf(pattern, version)

	log.WithFields(log.Fields{
		"path": path,
		"uri":  request.RequestURI,
	}).Debug("Accessing schema")
	http.ServeFile(writer, request, path)
}

func LoadConfig() {
	if err := config.Load(
		file.NewSource(file.WithPath("defaults.yaml")),
		file.NewSource(file.WithPath("config.yaml")),
		env.NewSource(env.WithStrippedPrefix("CC")),
	); err != nil {
		panic(fmt.Sprintf("Could not load configuration: %s", err))
	}
}

func ConfigureLogging() {
	key := "log"
	disableTimestamps := !config.Get(key, "timestamps").Bool(false)
	formatter := config.Get(key, "formatter").String("json")
	switch formatter {
	case "json":
		log.SetFormatter(&log.JSONFormatter{DisableTimestamp: disableTimestamps})
	case "text":
		log.SetFormatter(&log.TextFormatter{DisableTimestamp: disableTimestamps, FullTimestamp: true})
	default:
		log.Warnf("Log formatter '%s' is not supported. Using default", formatter)
	}

	log.SetOutput(os.Stdout)
	log.SetReportCaller(config.Get(key, "caller").Bool(false))

	level, err := log.ParseLevel(config.Get(key, "level").String("info"))
	if err != nil {
		log.Warnf("%s. Using info.", err)
		log.SetLevel(log.InfoLevel)
	} else {
		log.SetLevel(level)
	}
}
