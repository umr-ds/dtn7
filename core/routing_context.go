package core

import (
	"encoding/json"
	"fmt"
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

type ContextConfig struct {
	// ScriptPath is the path to the file which contains the javascript source for context evaluation
	// The file content needs to be encoded in utf-8
	ScriptPath string
	// ListenAddress if the address to which the context-update REST-interface should bind
	ListenAddress string
}

type ContextRouting struct {
	c                *Core
	contextSemaphore sync.RWMutex
	// context is this node's context information.
	// The map key is the name of information, the value has to be a JSON-encoded string
	context map[string]string
	// contextModified is true is this node has had a context update since the last broadcast, false otherwise
	contextModified bool
	// contains peer context data, the keys are nodeIDs, and the values are the same construct as our own context map
	peerContext map[string]map[string]string
	// javascript is a internal goja representation of the script that will be run for context evaluation
	javascript *goja.Program
	// address that broadcast bundles are sent to
	broadcastAddress bundle.EndpointID
}

func NewContextRouting(c *Core, config ContextConfig) *ContextRouting {
	log.Info("CONTEXT: Initialising Context Routing")
	contextRouting := ContextRouting{
		c:               c,
		context:         map[string]string{"NodeID": c.NodeId.String()},
		contextModified: false,
		peerContext:     make(map[string]map[string]string),
	}

	bAddress, err := bundle.NewEndpointID(BroadcastAddress)
	if err != nil {
		contextRouting.Fatal(log.Fields{
			"BroadcastAddress": BroadcastAddress,
			"error":            err,
		}, "Unable to parse broadcast address")
	}
	contextRouting.broadcastAddress = bAddress

	contextRouting.Info(nil, "Initialising Context REST-Interface")
	router := mux.NewRouter()
	router.HandleFunc("/context/{contextName}", contextRouting.contextUpdateHandler).Methods("POST")
	router.HandleFunc("/context", contextRouting.getHandler).Methods("GET")

	srv := &http.Server{
		Addr:         config.ListenAddress,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}
	go srv.ListenAndServe()
	contextRouting.Info(log.Fields{
		"address": config.ListenAddress,
	}, "Finished initialising Context REST-Interface")

	contextRouting.Info(nil, "Compiling javascript")
	dat, err := ioutil.ReadFile(config.ScriptPath)
	if err != nil {
		contextRouting.Fatal(log.Fields{
			"path":  config.ScriptPath,
			"error": err,
		}, "Error reading in script file")
	}

	text := string(dat)
	script, err := goja.Compile("context", text, false)
	if err != nil {
		contextRouting.Fatal(log.Fields{
			"script": text,
			"error":  err,
		}, "Error parsing javascript")
	}

	contextRouting.javascript = script
	contextRouting.Info(nil, "Compilation successful")

	// register our custom metadata-block
	extensionBlockManager := bundle.GetExtensionBlockManager()
	if !extensionBlockManager.IsKnown(ExtBlockTypeContextBlock) {
		// since we already checked if the block type exists, this really shouldn't ever fail...
		_ = extensionBlockManager.Register(NewNodeContextBlock(contextRouting.context))
	}

	contextRouting.Info(nil, "Setting up cron jobs")
	err = c.cron.Register("context_broadcast", contextRouting.broadcastCron, time.Minute)
	if err != nil {
		contextRouting.Warn(log.Fields{
			"reason": err,
		}, "Could not register broadcast cron")
	}

	contextRouting.Info(nil, "Finished Initialisation")
	return &contextRouting
}

func (contextRouting *ContextRouting) Fatal(fields log.Fields, message string) {
	if fields == nil {
		log.Fatal(fmt.Sprintf("CONTEXT: %s", message))
	} else {
		log.WithFields(fields).Fatal(fmt.Sprintf("CONTEXT: %s", message))
	}
}

func (contextRouting *ContextRouting) Warn(fields log.Fields, message string) {
	if fields == nil {
		log.Warn(fmt.Sprintf("CONTEXT: %s", message))
	} else {
		log.WithFields(fields).Warn(fmt.Sprintf("CONTEXT: %s", message))
	}
}

func (contextRouting *ContextRouting) Debug(fields log.Fields, message string) {
	if fields == nil {
		log.Debug(fmt.Sprintf("CONTEXT: %s", message))
	} else {
		log.WithFields(fields).Debug(fmt.Sprintf("CONTEXT: %s", message))
	}
}

func (contextRouting *ContextRouting) Info(fields log.Fields, message string) {
	if fields == nil {
		log.Info(fmt.Sprintf("CONTEXT: %s", message))
	} else {
		log.WithFields(fields).Info(fmt.Sprintf("CONTEXT: %s", message))
	}
}

func (contextRouting *ContextRouting) NotifyIncoming(bp BundlePack) {
	contextRouting.Debug(log.Fields{
		"bundle": bp.ID(),
	}, "Incoming bundle")

	bndl, err := bp.Bundle()
	if err != nil {
		contextRouting.Warn(log.Fields{
			"error": err.Error(),
		}, "Couldn't get bundle data")
		return
	}

	bi, err := contextRouting.c.store.QueryId(bp.Id)
	if err != nil {
		contextRouting.Warn(log.Fields{
			"error": err,
		}, "Failed to process a non-stored Bundle")
		return
	}

	metaDataBlock, err := bndl.ExtensionBlock(ExtBlockTypeContextBlock)
	// handle bundle context
	if err == nil {
		contextRouting.Debug(log.Fields{
			"bundle": bndl.ID(),
		}, "Bundle has context block")

		contextBlock := metaDataBlock.Value.(*ContextBlock)

		if contextBlock.Type == BundleContext {
			// this is a normal bundle
			contextRouting.Debug(log.Fields{
				"bundle": bndl.ID(),
			}, "Is normal bundle")

			context := contextBlock.Context

			contextRouting.Debug(log.Fields{
				"bundle":  bndl.ID(),
				"context": context,
			}, "Parsed bundle context")

			bi.Properties["routing/context/type"] = contextBlock.Type
			bi.Properties["routing/context/context"] = context
			bi.Properties["routing/context/source"] = bndl.PrimaryBlock.SourceNode.String()
			bi.Properties["routing/context/destination"] = bndl.PrimaryBlock.SourceNode.String()
		} else if contextBlock.Type == NodeContext {
			// this is a context bundle
			contextRouting.Debug(log.Fields{
				"bundle": bndl.ID(),
			}, "Is context bundle")

			// if it's from us, do nothing
			if bndl.PrimaryBlock.SourceNode != contextRouting.c.NodeId {
				peerID := bndl.PrimaryBlock.SourceNode
				context := contextBlock.Context

				contextRouting.Debug(log.Fields{
					"source":  peerID,
					"context": context,
				}, "Received peer context")

				contextRouting.contextSemaphore.Lock()
				contextRouting.peerContext[peerID.String()] = context
				contextRouting.contextSemaphore.Unlock()
			}

			bi.Properties["routing/context/type"] = contextBlock.Type
		} else {
			contextRouting.Warn(log.Fields{
				"contextBlock": contextBlock,
			}, "Unknown contextBlock-type")
		}
	}

	// Check if we got a PreviousNodeBlock and extract its EndpointID
	pnBlock, err := bndl.ExtensionBlock(bundle.ExtBlockTypePreviousNodeBlock)
	if err == nil {
		prevNode := pnBlock.Value.(*bundle.PreviousNodeBlock).Endpoint()

		sentEids, ok := bi.Properties["routing/context/sent"].([]bundle.EndpointID)
		if !ok {
			sentEids = make([]bundle.EndpointID, 0)
		}

		bi.Properties["routing/context/sent"] = append(sentEids, prevNode)
	}

	contextRouting.Debug(log.Fields{
		"bundle":     bndl.ID(),
		"properties": bi.Properties,
	}, "Updating bundle data in store")
	err = contextRouting.c.store.Update(bi)
	if err != nil {
		contextRouting.Warn(log.Fields{
			"error": err,
		}, "Failed to store bundle data")
	}
}

func (contextRouting *ContextRouting) DispatchingAllowed(bp BundlePack) bool {
	// TODO: Dummy Implementation
	return true
}

func (contextRouting *ContextRouting) SenderForBundle(bp BundlePack) (sender []cla.ConvergenceSender, delete bool) {
	bndl, err := bp.Bundle()
	if err != nil {
		contextRouting.Warn(log.Fields{
			"error": err.Error(),
		}, "Couldn't get bundle data")
		return
	}

	bi, err := contextRouting.c.store.QueryId(bp.Id)
	if err != nil {
		contextRouting.Warn(log.Fields{
			"bundle": bndl.ID(),
			"error":  err,
		}, "Failed to process a non-stored Bundle")
		return
	}

	bundleType, ok := bi.Properties["routing/context/type"]
	if !ok {
		contextRouting.Warn(log.Fields{
			"bundle":      bndl.ID(),
			"stored data": bi.Properties,
		}, "No bundle type stored")
		return nil, false
	}

	// handle context bundles
	if bundleType == NodeContext {
		if bndl.PrimaryBlock.SourceNode == contextRouting.c.NodeId {
			// if we are the originator of this bundle, we forward it to everyone and then we can directly delete it
			return contextRouting.c.claManager.Sender(), true
		} else {
			// if this is another node's context message, we do not forward it and delete it immediately
			return nil, true
		}
	}

	contextRouting.Debug(log.Fields{
		"bundle": bndl.ID(),
	}, "Initialising Javascript VM")
	vm := goja.New()
	vm.Set("loggingFunc", loggingFunc)

	bundleContext, ok := bi.Properties["routing/context/context"].(map[string]string)
	if !ok {
		contextRouting.Warn(log.Fields{
			"bundle":  bndl.ID(),
			"context": bi.Properties["routing/context/context"],
		}, "No context for bundle")
		return nil, false
	} else {
		contextRouting.Debug(log.Fields{
			"bundle":  bndl.ID(),
			"context": bundleContext,
		}, "Bundle Context")
	}
	vm.Set("bundleContext", bundleContext)

	source, ok := bi.Properties["routing/context/source"].(string)
	if !ok {
		contextRouting.Warn(log.Fields{
			"bundle": bndl.ID(),
			"source": bi.Properties["routing/context/source"],
		}, "Unable to get bundle source")
		return nil, false
	} else {
		contextRouting.Debug(log.Fields{
			"bundle": bndl.ID(),
			"source": source,
		}, "Bundle Source")
	}
	vm.Set("source", source)

	destination, ok := bi.Properties["routing/context/destination"].(string)
	if !ok {
		contextRouting.Warn(log.Fields{
			"bundle":      bndl.ID(),
			"destination": bi.Properties["routing/context/destination"],
		}, "Unable to get bundle destination")
		return nil, false
	} else {
		contextRouting.Debug(log.Fields{
			"bundle":      bndl.ID(),
			"destination": destination,
		}, "Bundle Destination")
	}
	vm.Set("destination", destination)

	contextRouting.contextSemaphore.RLock()
	context := contextRouting.context
	peerContext := contextRouting.peerContext
	peers := senderNames(contextRouting.c.claManager.Sender())
	contextRouting.contextSemaphore.RUnlock()

	vm.Set("context", context)
	vm.Set("peerContext", peerContext)
	vm.Set("peers", peers)
	contextRouting.Debug(log.Fields{
		"bundle": bndl.ID(),
	}, "Finished VM initialisation")
	result, err := vm.RunProgram(contextRouting.javascript)
	if err != nil {
		contextRouting.Warn(log.Fields{
			"bundle": bndl.ID(),
			"error":  err,
		}, "Error executing javascript")
		return
	}

	selected := make([]string, len(peers))
	err = vm.ExportTo(result, &selected)
	if err != nil {
		contextRouting.Warn(log.Fields{
			"bundle": bndl.ID(),
			"error":  err,
		}, "Could not export javascript return to string array")
		return
	}
	contextRouting.Debug(log.Fields{
		"bundle":  bndl.ID(),
		"senders": selected,
	}, "Javascript returned selection of senders")

	selectedSenders := contextRouting.getSendersWithMatchingIDs(contextRouting.c.claManager.Sender(), selected)

	contextRouting.Debug(log.Fields{
		"bundle": bndl.ID(),
		"CLAs":   selectedSenders,
	}, "")

	return selectedSenders, false
}

func (contextRouting *ContextRouting) ReportFailure(bp BundlePack, sender cla.ConvergenceSender) {
	// TODO: Dummy Implementation
}

func (contextRouting *ContextRouting) ReportPeerAppeared(peer cla.Convergence) {
	contextRouting.Debug(log.Fields{
		"address": peer,
	}, "Peer appeared")

	peerReceiver, ok := peer.(cla.ConvergenceSender)
	if !ok {
		contextRouting.Warn(nil, "Peer was not a ConvergenceSender")
		return
	}

	peerID := peerReceiver.GetPeerEndpointID()

	contextRouting.Debug(log.Fields{
		"peer": peerID,
	}, "PeerID discovered")

	// send the peer our context data
	contextRouting.contextSemaphore.RLock()
	contextBlock := NewNodeContextBlock(contextRouting.context)
	contextRouting.contextSemaphore.RUnlock()

	err := sendMetadataBundle(contextRouting.c, contextRouting.c.NodeId, peerID, contextBlock)
	if err != nil {
		contextRouting.Warn(log.Fields{
			"error": err,
		}, "Unable to send context block")
	}
}

func (contextRouting *ContextRouting) ReportPeerDisappeared(peer cla.Convergence) {
	// TODO: Dummy Implementation
}

// contextUpdateHandler handles context updates sent to the REST-interface.
// Updates are sent to the /context/{contextName} endpoint via POST.
// The request body has to be JSON encoded
func (contextRouting *ContextRouting) contextUpdateHandler(w http.ResponseWriter, r *http.Request) {
	contextRouting.Debug(nil, "Received context update")
	name := mux.Vars(r)["contextName"]

	bodyBinary, err := ioutil.ReadAll(r.Body)
	if err != nil {
		contextRouting.Warn(log.Fields{
			"error": err,
		}, "Unable to read request body")
		w.WriteHeader(http.StatusInternalServerError)
		_, err = w.Write([]byte("Internal Server Error"))
		if err != nil {
			contextRouting.Warn(log.Fields{
				"error": err,
			}, "An error occurred, while handling the error...")
		}
		return
	}

	// check, if it's valid json
	var parsed json.RawMessage
	err = json.Unmarshal(bodyBinary, &parsed)
	if err != nil {
		contextRouting.Info(log.Fields{
			"error": err,
		}, "Received invalid context update")
		w.WriteHeader(http.StatusBadRequest)
		_, err = w.Write([]byte(err.Error()))
		if err != nil {
			contextRouting.Warn(log.Fields{
				"error": err,
			}, "An error occurred, while handling the error...")
		}
		return
	}

	body := string(bodyBinary)
	contextRouting.contextSemaphore.Lock()
	contextRouting.context[name] = body
	contextRouting.contextModified = true
	contextRouting.contextSemaphore.Unlock()

	w.WriteHeader(http.StatusAccepted)
	contextRouting.Debug(log.Fields{
		"context": contextRouting.context,
	}, "Successfully updated Context")
}

func (contextRouting *ContextRouting) getHandler(w http.ResponseWriter, r *http.Request) {
	contextRouting.Debug(nil, "Received context request")

	context, err := json.Marshal(contextRouting.context)
	if err != nil {
		contextRouting.Warn(log.Fields{
			"error": err,
		}, "Unable to marshal context data")
		w.WriteHeader(http.StatusInternalServerError)
		_, err = w.Write([]byte(err.Error()))
		if err != nil {
			contextRouting.Warn(log.Fields{
				"error": err,
			}, "An error occurred, while handling the error...")
		}
		return
	}

	contextRouting.Debug(log.Fields{
		"context": context,
	}, "Marshaled context data")
	_, err = w.Write(context)
	if err != nil {
		contextRouting.Warn(log.Fields{
			"error": err,
		}, "Error writing response data")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func (contextRouting *ContextRouting) broadcastCron() {
	contextRouting.Debug(nil, "Running broadcast cron")
	contextRouting.contextSemaphore.RLock()
	// do nothing if nothing has changed
	if !contextRouting.contextModified {
		contextRouting.contextSemaphore.RUnlock()
		contextRouting.Debug(nil, "Context not modified, nothing to do")
		return
	}

	contextBlock := NewNodeContextBlock(contextRouting.context)
	contextRouting.contextSemaphore.RUnlock()

	contextRouting.Debug(log.Fields{
		"context": contextBlock,
	}, "Broadcasting context")

	err := sendMetadataBundle(contextRouting.c, contextRouting.c.NodeId, contextRouting.broadcastAddress, contextBlock)
	if err != nil {
		contextRouting.Warn(log.Fields{
			"error": err,
		}, "Error sending context broadcast")
		return
	}

	contextRouting.contextSemaphore.Lock()
	contextRouting.contextModified = false
	contextRouting.contextSemaphore.Unlock()
}

// senderNames converts a slice of ConvergenceSenders into a slice
func senderNames(senders []cla.ConvergenceSender) []string {
	names := make([]string, len(senders))
	for i := 0; i < len(senders); i++ {
		names[i] = senders[i].GetPeerEndpointID().String()
	}
	return names
}

// getSendersWithMatchingIDs takes the complete list of all senders and returns those with names matching 'selected'
func (contextRouting *ContextRouting) getSendersWithMatchingIDs(senders []cla.ConvergenceSender, selected []string) []cla.ConvergenceSender {
	contextRouting.Debug(log.Fields{
		"senders":  senders,
		"selected": selected,
	}, "Doing reverse cla name-id-lookup")

	sendersMap := make(map[string]cla.ConvergenceSender, len(senders))
	for i := 0; i < len(senders); i++ {
		name := senders[i].GetPeerEndpointID().String()
		sendersMap[name] = senders[i]
	}

	contextRouting.Debug(log.Fields{
		"sendersMap": sendersMap,
	}, "")

	selectedSenders := make([]cla.ConvergenceSender, len(selected))
	for i := 0; i < len(selected); i++ {
		if selected[i] != "" {
			selectedSenders[i] = sendersMap[selected[i]]
		}
	}

	contextRouting.Debug(log.Fields{
		"selectedSenders": selectedSenders,
	}, "Reverse lookup complete")

	return selectedSenders
}

func loggingFunc(message string) {
	log.Debug(fmt.Sprintf("JAVASCRIPT: %s", message))
}

const ExtBlockTypeContextBlock uint64 = 35043

const (
	NodeContext   = iota
	BundleContext = iota
)

type ContextBlock struct {
	Type    uint64
	Context map[string]string
}

func NewNodeContextBlock(context map[string]string) *ContextBlock {
	contextBlock := ContextBlock{
		Type:    NodeContext,
		Context: context,
	}
	return &contextBlock
}

func NewBundleContextBlock(context map[string]string) *ContextBlock {
	contextBlock := ContextBlock{
		Type:    BundleContext,
		Context: context,
	}
	return &contextBlock
}

func (contextBlock *ContextBlock) BlockTypeCode() uint64 {
	return ExtBlockTypeContextBlock
}

func (contextBlock *ContextBlock) CheckValid() error {
	return nil
}

func (contextBlock *ContextBlock) MarshalCbor(w io.Writer) error {
	err := cboring.WriteArrayLength(2, w)
	if err != nil {
		return err
	}

	err = cboring.WriteUInt(contextBlock.Type, w)
	if err != nil {
		return err
	}

	err = cboring.WriteMapPairLength(uint64(len(contextBlock.Context)), w)
	if err != nil {
		return err
	}

	for key, value := range contextBlock.Context {
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
	structLength, err := cboring.ReadArrayLength(r)
	if err != nil {
		return err
	} else if structLength != 2 {
		return fmt.Errorf("expected 2 fields, got %d", structLength)
	}

	contextType, err := cboring.ReadUInt(r)
	if err != nil {
		return err
	}

	contextBlock.Type = contextType

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

	contextBlock.Context = context

	return nil
}
