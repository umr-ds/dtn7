// SPDX-FileCopyrightText: 2019, 2020, 2021 Markus Sommer
//
// SPDX-License-Identifier: GPL-3.0-or-later

package routing

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/dop251/goja"
	"github.com/dtn7/dtn7-go/pkg/bpv7"
	"github.com/dtn7/dtn7-go/pkg/cla"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
)

const cadrBroadcastAddress = "dtn://routing/cadr/broadcast/"

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
	broadcastAddress bpv7.EndpointID
}

func NewContextRouting(c *Core, config ContextConfig) *ContextRouting {
	log.Info("CONTEXT: Initialising Context Routing")
	contextRouting := ContextRouting{
		c:               c,
		context:         make(map[string]string),
		contextModified: false,
		peerContext:     make(map[string]map[string]string),
	}

	bAddress, err := bpv7.NewEndpointID(cadrBroadcastAddress)
	if err != nil {
		contextRouting.Fatal(log.Fields{
			"BroadcastAddress": cadrBroadcastAddress,
			"error":            err,
		}, "Unable to parse broadcast address")
	}
	contextRouting.broadcastAddress = bAddress

	contextRouting.Info(nil, "Initialising Context REST-Interface")
	router := mux.NewRouter()
	router.HandleFunc("/rest/context/{contextName}", contextRouting.contextUpdateHandler).Methods("POST")
	router.HandleFunc("/rest/context", contextRouting.getHandler).Methods("GET")
	router.HandleFunc("/rest/size", contextRouting.storeSizeHandler).Methods("GET")

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
	extensionBlockManager := bpv7.GetExtensionBlockManager()
	if !extensionBlockManager.IsKnown(bpv7.ExtBlockTypeContextBlock) {
		// since we already checked if the block type exists, this really shouldn't ever fail...
		_ = extensionBlockManager.Register(bpv7.NewNodeContextBlock(contextRouting.context))
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

func (contextRouting *ContextRouting) RLock(caller string, arg interface{}) {
	/*if bid, ok := arg.(bundle.BundleID); ok {
		contextRouting.Debug(log.Fields{
			"method": caller,
			"type":   "read",
			"bundle": bid.String(),
		}, "Attempting to gain lock")
	} else if eid, ok := arg.(bundle.EndpointID); ok {
		contextRouting.Debug(log.Fields{
			"method": caller,
			"type":   "read",
			"peer":   eid.String(),
		}, "Attempting to gain lock")
	} else {
		contextRouting.Debug(log.Fields{
			"method": caller,
			"type":   "read",
		}, "Attempting to gain lock")
	}*/
	contextRouting.contextSemaphore.RLock()
	/*if bid, ok := arg.(bundle.BundleID); ok {
		contextRouting.Debug(log.Fields{
			"method": caller,
			"type":   "read",
			"bundle": bid.String(),
		}, "Gained lock")
	} else if eid, ok := arg.(bundle.EndpointID); ok {
		contextRouting.Debug(log.Fields{
			"method": caller,
			"type":   "read",
			"peer":   eid.String(),
		}, "Gained lock")
	} else {
		contextRouting.Debug(log.Fields{
			"method": caller,
			"type":   "read",
		}, "Gained lock")
	}*/
}

func (contextRouting *ContextRouting) RUnlock(caller string, arg interface{}) {
	/*if bid, ok := arg.(bundle.BundleID); ok {
		contextRouting.Debug(log.Fields{
			"method": caller,
			"type":   "read",
			"bundle": bid.String(),
		}, "Releasing lock")
	} else if eid, ok := arg.(bundle.EndpointID); ok {
		contextRouting.Debug(log.Fields{
			"method": caller,
			"type":   "read",
			"peer":   eid.String(),
		}, "Releasing lock")
	} else {
		contextRouting.Debug(log.Fields{
			"method": caller,
			"type":   "read",
		}, "Releasing lock")
	}*/
	contextRouting.contextSemaphore.RUnlock()
	/*if bid, ok := arg.(bundle.BundleID); ok {
		contextRouting.Debug(log.Fields{
			"method": caller,
			"type":   "read",
			"bundle": bid.String(),
		}, "Released lock")
	} else if eid, ok := arg.(bundle.EndpointID); ok {
		contextRouting.Debug(log.Fields{
			"method": caller,
			"type":   "read",
			"peer":   eid.String(),
		}, "Released lock")
	} else {
		contextRouting.Debug(log.Fields{
			"method": caller,
			"type":   "read",
		}, "Released lock")
	}*/
}

func (contextRouting *ContextRouting) Lock(caller string, arg interface{}) {
	/*if bid, ok := arg.(bundle.BundleID); ok {
		contextRouting.Debug(log.Fields{
			"method": caller,
			"type":   "write",
			"bundle": bid.String(),
		}, "Attempting to gain lock")
	} else if eid, ok := arg.(bundle.EndpointID); ok {
		contextRouting.Debug(log.Fields{
			"method": caller,
			"type":   "write",
			"peer":   eid.String(),
		}, "Attempting to gain lock")
	} else {
		contextRouting.Debug(log.Fields{
			"method": caller,
			"type":   "write",
		}, "Attempting to gain lock")
	}*/
	contextRouting.contextSemaphore.Lock()
	/*if bid, ok := arg.(bundle.BundleID); ok {
		contextRouting.Debug(log.Fields{
			"method": caller,
			"type":   "write",
			"bundle": bid.String(),
		}, "Gained lock")
	} else if eid, ok := arg.(bundle.EndpointID); ok {
		contextRouting.Debug(log.Fields{
			"method": caller,
			"type":   "write",
			"peer":   eid.String(),
		}, "Gained lock")
	} else {
		contextRouting.Debug(log.Fields{
			"method": caller,
			"type":   "write",
		}, "Gained lock")
	}*/
}

func (contextRouting *ContextRouting) Unlock(caller string, arg interface{}) {
	/*if bid, ok := arg.(bundle.BundleID); ok {
		contextRouting.Debug(log.Fields{
			"method": caller,
			"type":   "write",
			"bundle": bid.String(),
		}, "Releasing lock")
	} else if eid, ok := arg.(bundle.EndpointID); ok {
		contextRouting.Debug(log.Fields{
			"method": caller,
			"type":   "write",
			"peer":   eid.String(),
		}, "Releasing lock")
	} else {
		contextRouting.Debug(log.Fields{
			"method": caller,
			"type":   "write",
		}, "Releasing lock")
	}*/
	contextRouting.contextSemaphore.Unlock()
	/*if bid, ok := arg.(bundle.BundleID); ok {
		contextRouting.Debug(log.Fields{
			"method": caller,
			"type":   "write",
			"bundle": bid.String(),
		}, "Released lock")
	} else if eid, ok := arg.(bundle.EndpointID); ok {
		contextRouting.Debug(log.Fields{
			"method": caller,
			"type":   "write",
			"peer":   eid.String(),
		}, "Released lock")
	} else {
		contextRouting.Debug(log.Fields{
			"method": caller,
			"type":   "write",
		}, "Released lock")
	}*/
}

func (contextRouting *ContextRouting) NotifyNewBundle(bp BundleDescriptor) {
	contextRouting.Debug(log.Fields{
		"bundle": bp.ID(),
	}, "Incoming bundle")

	bndl, err := bp.Bundle()
	if err != nil {
		contextRouting.Warn(log.Fields{
			"error": err,
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

	metaDataBlock, err := bndl.ExtensionBlock(bpv7.ExtBlockTypeContextBlock)
	// handle bundle context
	if err == nil {
		contextRouting.Debug(log.Fields{
			"bundle": bndl.ID(),
		}, "Bundle has context block")

		contextBlock := metaDataBlock.Value.(*bpv7.ContextBlock)

		if contextBlock.Type == bpv7.BundleContext {
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
		} else if contextBlock.Type == bpv7.NodeContext {
			// this is a context bundle
			contextRouting.Debug(log.Fields{
				"bundle": bp.ID(),
			}, "Is context bundle")

			// if it's from us, do nothing
			if bndl.PrimaryBlock.SourceNode != contextRouting.c.NodeId {
				peerID := bndl.PrimaryBlock.SourceNode
				context := contextBlock.Context

				contextRouting.Debug(log.Fields{
					"source":  peerID,
					"context": context,
				}, "Received peer context")

				contextRouting.Lock("NotifyIncoming", bp.Id)
				contextRouting.peerContext[peerID.String()] = context
				contextRouting.Unlock("NotifyIncoming", bp.Id)
			}

			bi.Properties["routing/context/type"] = contextBlock.Type
		} else {
			contextRouting.Warn(log.Fields{
				"contextBlock": contextBlock,
			}, "Unknown contextBlock-type")
		}
	}

	// Check if we got a PreviousNodeBlock and extract its EndpointID
	pnBlock, err := bndl.ExtensionBlock(bpv7.ExtBlockTypePreviousNodeBlock)
	if err == nil {
		prevNode := pnBlock.Value.(*bpv7.PreviousNodeBlock).Endpoint()

		log.WithFields(log.Fields{
			"bundle": bndl.ID(),
			"src":    prevNode,
		}).Info("Received bundle from peer")

		sentEids, ok := bi.Properties["routing/context/sent"].([]bpv7.EndpointID)
		if !ok {
			sentEids = make([]bpv7.EndpointID, 0)
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

func (contextRouting *ContextRouting) DispatchingAllowed(_ BundleDescriptor) bool {
	// TODO: Dummy Implementation
	return true
}

func (contextRouting *ContextRouting) SenderForBundle(bp BundleDescriptor) (sender []cla.ConvergenceSender, delete bool) {
	contextRouting.Debug(log.Fields{
		"bundle": bp.ID(),
	}, "Starting routing decision")

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
	if bundleType == bpv7.NodeContext {
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
	vm.Set("bndl", bndl)
	vm.Set("modifyBundleContext", contextRouting.modifyBundleContext)
	vm.Set("bundleID", bp.ID())

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
			"bundle":      bp.ID(),
			"destination": bi.Properties["routing/context/destination"],
		}, "Unable to get bundle destination")
		return nil, false
	} else {
		contextRouting.Debug(log.Fields{
			"bundle":      bp.ID(),
			"destination": destination,
		}, "Bundle Destination")
	}
	vm.Set("destination", destination)

	contextRouting.RLock("SenderForBundle", bp.Id)
	context := contextRouting.context
	peerContext := contextRouting.peerContext
	contextRouting.RUnlock("SenderForBundle", bp.Id)

	validPeers, _ := filterCLAs(bi, contextRouting.c.claManager.Sender(), "context")
	if len(validPeers) == 0 {
		contextRouting.Debug(log.Fields{
			"bundle": bp.ID(),
		}, "All peers already received bundle")
		return validPeers, false
	}
	peers := senderNames(validPeers)

	vm.Set("context", context)
	vm.Set("peerContext", peerContext)
	vm.Set("peers", peers)
	contextRouting.Debug(log.Fields{
		"bundle": bp.ID(),
	}, "Finished VM initialisation")
	result, err := vm.RunProgram(contextRouting.javascript)
	if err != nil {
		contextRouting.Warn(log.Fields{
			"bundle": bp.ID(),
			"error":  err,
		}, "Error executing javascript")
		return
	}

	selected := make([]string, len(peers))
	err = vm.ExportTo(result, &selected)
	if err != nil {
		contextRouting.Warn(log.Fields{
			"bundle": bp.ID(),
			"error":  err,
		}, "Could not export javascript return to string array")
		return
	}
	contextRouting.Debug(log.Fields{
		"bundle":  bp.ID(),
		"senders": selected,
	}, "Javascript returned selection of senders")

	selectedSenders := contextRouting.getSendersWithMatchingIDs(contextRouting.c.claManager.Sender(), selected)

	contextRouting.Debug(log.Fields{
		"bundle": bp.ID(),
		"CLAs":   selectedSenders,
	}, "ContextRouting selected senders.")

	filtered, sentEIDs := filterCLAs(bi, selectedSenders, "context")

	if len(filtered) == 0 {
		contextRouting.Debug(log.Fields{
			"bundle": bp.ID(),
		}, "All selected peers already received bundle")
		return filtered, false
	}

	bi.Properties["routing/context/sent"] = sentEIDs
	if err := contextRouting.c.store.Update(bi); err != nil {
		contextRouting.Warn(log.Fields{
			"bundle": bp.ID(),
			"error":  err,
		}, "Updating BundleItem failed")
	}

	contextRouting.Debug(log.Fields{
		"bundle": bp.ID(),
		"peers":  filtered,
	}, "Routing decision finished")

	return filtered, false
}

func (contextRouting *ContextRouting) ReportFailure(bp BundleDescriptor, sender cla.ConvergenceSender) {
	contextRouting.Debug(log.Fields{
		"bundle": bp.ID(),
		"peer":   sender.Address(),
	}, "Sending bundle failed")
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
	contextRouting.RLock("ReportPeerAppeared", peerID)
	context := contextRouting.context
	contextRouting.RUnlock("ReportPeerAppeared", peerID)

	if len(context) == 0 {
		// if there is no context to broadcast, then don't
		contextRouting.Debug(nil, "No context to broadcast")
		return
	}

	contextRouting.Lock("ReportPeerAppeared", peerID)
	contextRouting.context["NodeID"] = contextRouting.c.NodeId.String()
	contextRouting.Unlock("ReportPeerAppeared", peerID)

	contextBlock := bpv7.NewNodeContextBlock(context)

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

// modifyBundleContext allows for the modification (overwriting) of a bundle's context block
// To be passed to the script vm to enable full software defined routing
func (contextRouting *ContextRouting) modifyBundleContext(bndl *bpv7.Bundle, context map[string]string) {
	_, err := bndl.ExtensionBlock(bpv7.ExtBlockTypeContextBlock)
	if err != nil {
		bndl.RemoveExtensionBlockByBlockNumber(bpv7.ExtBlockTypeContextBlock)
	}

	contextBlock := bpv7.NewBundleContextBlock(context)
	bndl.AddExtensionBlock(bpv7.NewCanonicalBlock(0, 0, contextBlock))
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

	contextRouting.Lock("contextUpdateHandler", "")
	contextRouting.context[name] = body
	contextRouting.contextModified = true
	contextRouting.Unlock("contextUpdateHandler", "")

	w.WriteHeader(http.StatusAccepted)
	contextRouting.Debug(log.Fields{
		"context": contextRouting.context,
	}, "Successfully updated Context")
}

func (contextRouting *ContextRouting) getHandler(w http.ResponseWriter, _ *http.Request) {
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

func (contextRouting *ContextRouting) storeSizeHandler(w http.ResponseWriter, _ *http.Request) {
	contextRouting.Debug(nil, "Received size request")

	pending, err := contextRouting.c.store.QueryPending()
	if err != nil {
		contextRouting.Warn(log.Fields{
			"error": err,
		}, "Error querying pending")
		w.WriteHeader(http.StatusInternalServerError)
		_, err = w.Write([]byte(err.Error()))
		if err != nil {
			contextRouting.Warn(log.Fields{
				"error": err,
			}, "An error occurred, while handling the error...")
		}
		return
	}

	size := len(pending)
	contextRouting.Debug(log.Fields{
		"size": size,
	}, "Size of pending buffer")

	_, err = w.Write([]byte(strconv.Itoa(size)))
	if err != nil {
		contextRouting.Warn(log.Fields{
			"error": err,
		}, "Error writing response")

		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func (contextRouting *ContextRouting) broadcastCron() {
	contextRouting.Debug(nil, "Running broadcast cron")

	contextRouting.RLock("broadcastCron-1", "")
	context := contextRouting.context
	contextModified := contextRouting.contextModified
	contextRouting.RUnlock("broadcastCron-1", "")

	if len(context) == 0 {
		// if there is no context to broadcast, then don't
		contextRouting.Debug(nil, "No context to broadcast")
		return
	}

	// do nothing if nothing has changed
	if !contextModified {
		contextRouting.Debug(nil, "Context not modified, nothing to do")
		return
	}

	contextRouting.Lock("broadcastCron-1", "")
	contextRouting.context["NodeID"] = contextRouting.c.NodeId.String()
	contextRouting.Unlock("broadcastCron-1", "")

	contextBlock := bpv7.NewNodeContextBlock(context)

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

	contextRouting.Lock("broadcastCron-2", "")
	contextRouting.contextModified = false
	contextRouting.Unlock("broadcastCron-2", "")
}

// senderNames converts a slice of ConvergenceSenders into a slice of strings
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
