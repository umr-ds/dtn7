package core

import (
	"encoding/json"
	"fmt"
	"github.com/dtn7/dtn7-go/bundle"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
	"net/http"
	"sync"
	"time"
)

type sendRequest struct {
	Recipient string
	Context   map[string]string
	Payload   []byte
}

type ContextRESTAgend struct {
	c           *Core
	address     string
	endpointID  bundle.EndpointID
	bundleIds   []bundle.BundleID
	bundleMutex sync.Mutex
}

func NewContextAgent(c *Core, address string) *ContextRESTAgend {
	log.Info("Initialising ContextRestAgent")
	agent := ContextRESTAgend{
		c:          c,
		address:    address,
		endpointID: c.NodeId,
		bundleIds:  make([]bundle.BundleID, 0),
	}

	router := mux.NewRouter()
	router.HandleFunc("/send", agent.sendHandler).Methods("POST")
	srv := &http.Server{
		Addr:         agent.address,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}
	go srv.ListenAndServe()

	agent.Info(nil, "Initialisation successful")
	return &agent
}

func (agent *ContextRESTAgend) Fatal(fields log.Fields, message string) {
	if fields == nil {
		log.Fatal(fmt.Sprintf("CONTEXTAGENT: %s", message))
	} else {
		log.WithFields(fields).Fatal(fmt.Sprintf("CONTEXTAGENT: %s", message))
	}
}

func (agent *ContextRESTAgend) Warn(fields log.Fields, message string) {
	if fields == nil {
		log.Warn(fmt.Sprintf("CONTEXTAGENT: %s", message))
	} else {
		log.WithFields(fields).Warn(fmt.Sprintf("CONTEXTAGENT: %s", message))
	}
}

func (agent *ContextRESTAgend) Debug(fields log.Fields, message string) {
	if fields == nil {
		log.Debug(fmt.Sprintf("CONTEXTAGENT: %s", message))
	} else {
		log.WithFields(fields).Debug(fmt.Sprintf("CONTEXTAGENT: %s", message))
	}
}

func (agent *ContextRESTAgend) Info(fields log.Fields, message string) {
	if fields == nil {
		log.Info(fmt.Sprintf("CONTEXTAGENT: %s", message))
	} else {
		log.WithFields(fields).Info(fmt.Sprintf("CONTEXTAGENT: %s", message))
	}
}

func (agent *ContextRESTAgend) EndpointID() bundle.EndpointID {
	return agent.endpointID
}

func (agent *ContextRESTAgend) Deliver(bp BundlePack) error {
	agent.Debug(log.Fields{
		"bundle": bp.ID(),
	}, "Received a bundle")

	agent.bundleMutex.Lock()
	agent.bundleIds = append(agent.bundleIds, bp.Id)
	agent.bundleMutex.Unlock()

	return nil
}

func (agent *ContextRESTAgend) sendHandler(w http.ResponseWriter, r *http.Request) {
	agent.Debug(nil, "Received bundle")

	decorder := json.NewDecoder(r.Body)
	message := sendRequest{}

	err := decorder.Decode(&message)
	if err != nil {
		agent.Info(log.Fields{
			"error": err,
		}, "Received invalid message request")
		w.WriteHeader(http.StatusBadRequest)
		_, err = w.Write([]byte(err.Error()))
		if err != nil {
			agent.Warn(log.Fields{
				"error": err,
			}, "An error occurred, while handling the error...")
		}
		return
	} else {
		agent.Debug(log.Fields{
			"message": message,
		}, "Decoded message")
	}

	recipient, err := bundle.NewEndpointID(message.Recipient)
	if err != nil {
		agent.Info(log.Fields{
			"error": err,
		}, "Invalid EndpointID")
		w.WriteHeader(http.StatusBadRequest)
		_, err = w.Write([]byte(err.Error()))
		if err != nil {
			agent.Warn(log.Fields{
				"error": err,
			}, "An error occurred, while handling the error...")
		}
		return
	}

	builder := bundle.Builder()
	builder.Source(agent.endpointID)
	builder.Destination(recipient)
	builder.CreationTimestampNow()
	builder.Lifetime("60m")
	builder.BundleCtrlFlags(bundle.MustNotFragmented | bundle.StatusRequestDelivery)
	builder.PayloadBlock(message.Payload)
	builder.Canonical(NewBundleContextBlock(message.Context))
	bndl, err := builder.Build()
	if err != nil {
		agent.Info(log.Fields{
			"error": err,
		}, "Invalid EndpointID")
		w.WriteHeader(http.StatusInternalServerError)
		_, err = w.Write([]byte(err.Error()))
		if err != nil {
			agent.Warn(log.Fields{
				"error": err,
			}, "An error occurred, while handling the error...")
		}
		return
	}

	agent.c.SendBundle(&bndl)

	w.WriteHeader(http.StatusAccepted)
}
