package core

import (
	"encoding/json"
	"github.com/dop251/goja"
	"github.com/dtn7/cboring"
	"github.com/dtn7/dtn7-go/bundle"
	"github.com/dtn7/dtn7-go/cla"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
	"io"
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
	// contains peer context data, the keys are nodeIDs, and the values are the same construct as our own context map
	peerContext map[string]map[string]string
	// javascriptVM is the javascript interpreter which executes the context evaluation-code
	javascriptVM *goja.Runtime
	// the javascript interpreter is not thread safe, but reinstantiating it might be a bit much of an overhead
	// so we serialise interpreter access with a semaphore
	vmSemaphore sync.Mutex
}

func NewContextRouting(c *Core) *ContextRouting {
	log.Info("Initialising ContextRouting")
	contextRouting := ContextRouting{
		c:            c,
		context:      make(map[string]string),
		peerContext:  make(map[string]map[string]string),
		javascriptVM: goja.New(),
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

	log.Info("Initialising javascript interpreter")
	nodeID := c.NodeId.String()
	contextRouting.javascriptVM.ToValue(nodeID)
	log.Info("Finished initialising javascript interpreter")

	// register our custom metadata-block
	extensionBlockManager := bundle.GetExtensionBlockManager()
	if !extensionBlockManager.IsKnown(ExtBlockTypeContextBlock) {
		// since we already checked if the block type exists, this really shouldn't ever fail...
		_ = extensionBlockManager.Register(newContextBlock(contextRouting.context))
	}

	log.Info("Finished initialising ContextRouting")
	return &contextRouting
}

func (contextRouting *ContextRouting) NotifyIncoming(bp BundlePack) {
	bndl, err := bp.Bundle()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Warn("Couldn't get bundle data")
		return
	}

	// handle context bundles
	if metaDataBlock, err := bndl.ExtensionBlock(ExtBlockTypeProphetBlock); err == nil {
		// if it's from us, do nothing
		if bndl.PrimaryBlock.SourceNode == contextRouting.c.NodeId {
			return
		}

		peerID := bndl.PrimaryBlock.SourceNode

		log.WithFields(log.Fields{
			"source": peerID,
		}).Debug("Received peer context")

		contextBlock := metaDataBlock.Value.(*ContextBlock)
		peerContext := contextBlock.getContext()

		contextRouting.contextSemaphore.Lock()
		contextRouting.peerContext[peerID.String()] = peerContext
		contextRouting.contextSemaphore.Unlock()

		return
	}
}

func (contextRouting *ContextRouting) DispatchingAllowed(bp BundlePack) bool {
	// TODO: Dummy Implementation
	return true
}

func (contextRouting *ContextRouting) SenderForBundle(bp BundlePack) (sender []cla.ConvergenceSender, delete bool) {
	bndl, err := bp.Bundle()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Warn("Couldn't get bundle data")
		return
	}

	// handle context bundles
	if _, err := bndl.ExtensionBlock(ExtBlockTypeProphetBlock); err == nil {
		if bndl.PrimaryBlock.SourceNode == contextRouting.c.NodeId {
			// if we are the originator of this bundle, we forward it to everyone
			return contextRouting.c.claManager.Sender(), false
		} else {
			// if this is another node's context message, we do not forward it and delete it immediately
			return nil, true
		}
	}

	contextRouting.contextSemaphore.RLock()
	context := contextRouting.context
	peerContext := contextRouting.peerContext
	contextRouting.contextSemaphore.RUnlock()

	contextRouting.vmSemaphore.Lock()
	contextRouting.javascriptVM.ToValue(context)
	contextRouting.javascriptVM.ToValue(peerContext)
	contextRouting.vmSemaphore.Unlock()

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

const ExtBlockTypeContextBlock uint64 = 35043

type ContextBlock map[string]string

func newContextBlock(context map[string]string) *ContextBlock {
	contextBlock := ContextBlock(context)
	return &contextBlock
}

func (contextBlock *ContextBlock) getContext() map[string]string {
	return *contextBlock
}

func (contextBlock *ContextBlock) BlockTypeCode() uint64 {
	return ExtBlockTypeContextBlock
}

func (contextBlock *ContextBlock) CheckValid() error {
	return nil
}

func (contextBlock *ContextBlock) MarshalCbor(w io.Writer) error {
	err := cboring.WriteMapPairLength(uint64(len(*contextBlock)), w)
	if err != nil {
		return err
	}

	for key, value := range *contextBlock {
		err = cboring.WriteTextString(key, w)
		if err != nil {
			return err
		}

		err = cboring.WriteTextString(value, w)
		if err != nil {
			return err
		}
	}

	return nil
}

func (contextBlock *ContextBlock) UnmarshalCbor(r io.Reader) error {
	length, err := cboring.ReadMapPairLength(r)
	if err != nil {
		return err
	}

	context := make(map[string]string, length)
	var i uint64
	for i = 0; i < length; i++ {
		key, err := cboring.ReadTextString(r)
		if err != nil {
			return err
		}

		value, err := cboring.ReadTextString(r)
		if err != nil {
			return err
		}

		context[key] = value
	}

	*contextBlock = context

	return nil
}
