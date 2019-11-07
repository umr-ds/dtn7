package core

import (
	"encoding/json"
	"github.com/dtn7/dtn7-go/cla"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"sync"
	"time"
)

type ContextRouting struct {
	c                *Core
	contextSemaphore sync.RWMutex
	// context is this node's context information.
	// The map key is the name of information, the value has to be a JSON-encoded string
	context map[string]string
}

func NewContextRouting(c *Core) *ContextRouting {
	log.Info("Initialising ContextRouting")
	contextRouting := ContextRouting{
		c:       c,
		context: make(map[string]string),
	}

	log.Info("Initialising Context REST-Interface")
	router := mux.NewRouter()
	router.HandleFunc("/context/{contextName}", contextRouting.contextUpdateHandler).Methods("POST")
	srv := &http.Server{
		Addr:         "127.0.0.1:35043",
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}
	go srv.ListenAndServe()
	log.Info("Finished initialising Context REST-Interface")

	log.Info("Finished initialising ContextRouting")
	return &contextRouting
}

func (contextRouting *ContextRouting) NotifyIncoming(bp BundlePack) {
	// TODO: Dummy Implementation
}

func (contextRouting *ContextRouting) DispatchingAllowed(bp BundlePack) bool {
	// TODO: Dummy Implementation
	return true
}

func (contextRouting *ContextRouting) SenderForBundle(bp BundlePack) (sender []cla.ConvergenceSender, delete bool) {
	// TODO: Dummy Implementation
	return nil, false
}

func (contextRouting *ContextRouting) ReportFailure(bp BundlePack, sender cla.ConvergenceSender) {
	// TODO: Dummy Implementation
}

func (contextRouting *ContextRouting) ReportPeerAppeared(peer cla.Convergence) {
	// TODO: Dummy Implementation
}

func (contextRouting *ContextRouting) ReportPeerDisappeared(peer cla.Convergence) {
	// TODO: Dummy Implementation
}

// contextUpdateHandler handles context updates sent to the REST-interface.
// Updates are sent to the /context/{contextName} endpoint via POST.
// The request body has to be JSON encoded
func (contextRouting *ContextRouting) contextUpdateHandler(w http.ResponseWriter, r *http.Request) {
	log.Debug("CONTEXT_UPDATE: Received context update")
	name := mux.Vars(r)["contextName"]

	bodyBinary, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Warn("CONTEXT_UPDATE: Unable to read request body")
		w.WriteHeader(http.StatusInternalServerError)
		_, err = w.Write([]byte("Internal Server Error"))
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Warn("CONTEXT_UPDATE: An error occurred, while handling the error...")
		}
		return
	}

	// check, if it's valid json
	var parsed json.RawMessage
	err = json.Unmarshal(bodyBinary, &parsed)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Info("CONTEXT_UPDATE: Received invalid context update")
		w.WriteHeader(http.StatusBadRequest)
		_, err = w.Write([]byte(err.Error()))
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Warn("CONTEXT_UPDATE: An error occurred, while handling the error...")
		}
		return
	}

	body := string(bodyBinary)
	contextRouting.contextSemaphore.Lock()
	contextRouting.context[name] = body
	contextRouting.contextSemaphore.Unlock()

	w.WriteHeader(http.StatusAccepted)
	log.WithFields(log.Fields{
		"context": contextRouting.context,
	}).Debug("CONTEXT_UPDATE: Successfully updated Context")
}
