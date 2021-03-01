// SPDX-FileCopyrightText: 2019, 2020 Alvar Penning
// SPDX-FileCopyrightText: 2019, 2021 Markus Sommer
//
// SPDX-License-Identifier: GPL-3.0-or-later

package routing

import (
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/RyanCarrier/dijkstra"

	"github.com/dtn7/dtn7-go/pkg/bpv7"
	"github.com/dtn7/dtn7-go/pkg/cla"
)

const dtlsrBroadcastAddress = "dtn://routing/dtlsr/broadcast/"

type DTLSRConfig struct {
	// RecomputeTime is the interval (in seconds) until the routing table is recomputed.
	// Note: Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".
	RecomputeTime string
	// BroadcastTime is the interval (in seconds) between broadcasts of peer data.
	// Note: Broadcast only happens when there was a change in peer data.
	// Note: Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".
	BroadcastTime string
	// PurgeTime is the interval after which a disconnected peer is removed from the peer list.
	// Note: Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".
	PurgeTime string
}

// DTLSR is an implementation of "Delay Tolerant Link State Routing"
type DTLSR struct {
	c *Core
	// routingTable is a [endpoint]forwardingNode mapping
	routingTable map[bpv7.EndpointID]bpv7.EndpointID
	// peerChange denotes whether there has been a change in our direct connections
	// since we last calculated our routing table/broadcast our peer data
	peerChange bool
	// peers is our own peerData
	peers bpv7.DTLSRPeerData
	// receivedChange denotes whether we received new data since we last computed our routing table
	receivedChange bool
	// receivedData is peerData received from other nodes
	receivedData map[bpv7.EndpointID]bpv7.DTLSRPeerData
	// nodeIndex and index Node are a bidirectional mapping EndpointID <-> uint64
	// necessary since the dijkstra implementation only accepts integer node identifiers
	nodeIndex map[bpv7.EndpointID]int
	indexNode []bpv7.EndpointID
	length    int
	// broadcastAddress is where metadata-bundles are sent to
	broadcastAddress bpv7.EndpointID
	// purgeTime is the time until a peer gets removed from the peer list
	purgeTime time.Duration
	// dataMutex is a RW-mutex which protects change operations to the algorithm's metadata
	dataMutex sync.RWMutex
}

func NewDTLSR(c *Core, config DTLSRConfig) *DTLSR {
	log.WithFields(log.Fields{
		"config": config,
	}).Debug("Initialising DTLSR")

	bAddress, err := bpv7.NewEndpointID(dtlsrBroadcastAddress)
	if err != nil {
		log.WithFields(log.Fields{
			"dtlsrBroadcastAddress": dtlsrBroadcastAddress,
		}).Fatal("Unable to parse broadcast address")
	}

	purgeTime, err := time.ParseDuration(config.PurgeTime)
	if err != nil {
		log.WithFields(log.Fields{
			"string": config.PurgeTime,
		}).Fatal("Unable to parse duration")
	}

	dtlsr := DTLSR{
		c:            c,
		routingTable: make(map[bpv7.EndpointID]bpv7.EndpointID),
		peerChange:   false,
		peers: bpv7.DTLSRPeerData{
			ID:        c.NodeId,
			Timestamp: bpv7.DtnTimeNow(),
			Peers:     make(map[bpv7.EndpointID]bpv7.DtnTime),
		},
		receivedChange:   false,
		receivedData:     make(map[bpv7.EndpointID]bpv7.DTLSRPeerData),
		nodeIndex:        map[bpv7.EndpointID]int{c.NodeId: 0},
		indexNode:        []bpv7.EndpointID{c.NodeId},
		length:           1,
		broadcastAddress: bAddress,
		purgeTime:        purgeTime,
	}

	err = c.cron.Register("dtlsr_purge", dtlsr.purgePeers, purgeTime)
	if err != nil {
		log.WithFields(log.Fields{
			"reason": err.Error(),
		}).Warn("Could not register DTLSR purge job")
	}

	recomputeTime, err := time.ParseDuration(config.RecomputeTime)
	if err != nil {
		log.WithFields(log.Fields{
			"string": config.RecomputeTime,
		}).Fatal("Unable to parse duration")
	}

	err = c.cron.Register("dtlsr_recompute", dtlsr.recomputeCron, recomputeTime)
	if err != nil {
		log.WithFields(log.Fields{
			"reason": err.Error(),
		}).Warn("Could not register DTLSR recompute job")
	}

	broadcastTime, err := time.ParseDuration(config.BroadcastTime)
	if err != nil {
		log.WithFields(log.Fields{
			"string": config.BroadcastTime,
		}).Fatal("Unable to parse duration")
	}

	err = c.cron.Register("dtlsr_broadcast", dtlsr.broadcastCron, broadcastTime)
	if err != nil {
		log.WithFields(log.Fields{
			"reason": err.Error(),
		}).Warn("Could not register DTLSR broadcast job")
	}

	// register our custom metadata-block
	extensionBlockManager := bpv7.GetExtensionBlockManager()
	if !extensionBlockManager.IsKnown(bpv7.ExtBlockTypeDTLSRBlock) {
		// since we already checked if the block type exists, this really shouldn't ever fail...
		_ = extensionBlockManager.Register(bpv7.NewDTLSRBlock(dtlsr.peers))
	}

	return &dtlsr
}

func (dtlsr *DTLSR) NotifyNewBundle(bp BundleDescriptor) {
	log.WithFields(log.Fields{
		"bundle": bp.ID(),
	}).Debug("Incoming bundle")

	if metaDataBlock, err := bp.MustBundle().ExtensionBlock(bpv7.ExtBlockTypeDTLSRBlock); err == nil {
		log.WithFields(log.Fields{
			"peer": bp.MustBundle().PrimaryBlock.SourceNode,
		}).Debug("Received metadata")

		dtlsrBlock := metaDataBlock.Value.(*bpv7.DTLSRBlock)
		data := dtlsrBlock.GetPeerData()

		log.WithFields(log.Fields{
			"peer": bp.MustBundle().PrimaryBlock.SourceNode,
			"data": data,
		}).Debug("Decoded peer data")

		dtlsr.dataMutex.Lock()
		defer dtlsr.dataMutex.Unlock()
		storedData, present := dtlsr.receivedData[data.ID]

		if !present {
			log.Debug("Data for new peer")
			// if we didn't have any data for that peer, we simply add it
			dtlsr.receivedData[data.ID] = data
			dtlsr.receivedChange = true

			// track node
			dtlsr.newNode(data.ID)

			// track peers of this node
			for node := range data.Peers {
				dtlsr.newNode(node)
			}
		} else {
			// check if the received data is newer and replace it if it is
			if data.ShouldReplace(storedData) {
				log.Debug("Updating peer data")
				dtlsr.receivedData[data.ID] = data
				dtlsr.receivedChange = true

				// track peers of this node
				for node := range data.Peers {
					dtlsr.newNode(node)
				}
			}
		}
	}

	// store cla from which we received this bundle so that we don't always bounce bundles between nodes
	bundleItem, err := dtlsr.c.store.QueryId(bp.Id)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Debug("Bundle not in store")
		return
	}

	bndl := bp.MustBundle()

	if pnBlock, err := bndl.ExtensionBlock(bpv7.ExtBlockTypePreviousNodeBlock); err == nil {
		prevNode := pnBlock.Value.(*bpv7.PreviousNodeBlock).Endpoint()

		log.WithFields(log.Fields{
			"bundle": bndl.ID(),
			"src":    prevNode,
		}).Info("Received bundle from peer")

		sentEids, ok := bundleItem.Properties["routing/dtlsr/sent"].([]bpv7.EndpointID)
		if !ok {
			sentEids = make([]bpv7.EndpointID, 0)
		}

		bundleItem.Properties["routing/dtlsr/sent"] = append(sentEids, prevNode)
		if err := dtlsr.c.store.Update(bundleItem); err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Warn("Updating BundleItem failed")
		}
	}
}

func (_ *DTLSR) ReportFailure(_ BundleDescriptor, _ cla.ConvergenceSender) {
	// if the transmission failed, that is sad, but there is really nothing to do...
}

func (dtlsr *DTLSR) SenderForBundle(bp BundleDescriptor) (sender []cla.ConvergenceSender, delete bool) {
	log.WithFields(log.Fields{
		"bundle": bp.ID(),
	}).Debug("Starting routing decision")

	defer log.WithFields(log.Fields{
		"bundle": bp.ID(),
	}).Debug("Routing decision finished")

	delete = false

	bndl, err := bp.Bundle()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Debug("Bundle no longer exists")
		return
	}

	if bndl.PrimaryBlock.Destination == dtlsr.broadcastAddress {
		bundleItem, err := dtlsr.c.store.QueryId(bp.Id)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err.Error(),
			}).Debug("Bundle not in store")
			return
		}

		sender, sentEids := filterCLAs(bundleItem, dtlsr.c.claManager.Sender(), "dtlsr")

		// broadcast bundles are always forwarded to everyone
		log.WithFields(log.Fields{
			"bundle":    bndl.ID(),
			"recipient": bndl.PrimaryBlock.Destination,
			"CLAs":      sender,
		}).Debug("Relaying broadcast bundle")

		bundleItem.Properties["routing/dtlsr/sent"] = sentEids
		if err := dtlsr.c.store.Update(bundleItem); err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Warn("Updating BundleItem failed")
		}

		log.WithFields(log.Fields{
			"bundle": bndl.ID(),
			"peers":  sender,
		}).Debug("Forwarding metadata-bundle to theses peers.")
		return sender, delete
	}

	recipient := bndl.PrimaryBlock.Destination

	dtlsr.dataMutex.RLock()
	forwarder, present := dtlsr.routingTable[recipient]
	dtlsr.dataMutex.RUnlock()
	if !present {
		// we don't know where to forward this bundle
		log.WithFields(log.Fields{
			"bundle":    bp.ID(),
			"recipient": recipient,
		}).Debug("DTLSR could not find a node to forward to")
		return
	}

	for _, cs := range dtlsr.c.claManager.Sender() {
		if cs.GetPeerEndpointID() == forwarder {
			sender = append(sender, cs)
			log.WithFields(log.Fields{
				"bundle":             bndl.ID(),
				"recipient":          recipient,
				"convergence-sender": sender,
			}).Debug("DTLSR selected Convergence Sender for an outgoing bundle")
			// we only ever forward to a single node
			// since DTLSR has no multiplicity for bundles
			// (we only ever forward it to the next node according to our routing table),
			// we can delete the bundle from our store after successfully forwarding it
			delete = true
			return
		}
	}

	log.WithFields(log.Fields{
		"bundle":    bp.ID(),
		"recipient": recipient,
	}).Debug("DTLSR could not find forwarder amongst connected nodes")
	return
}

func (dtlsr *DTLSR) ReportPeerAppeared(peer cla.Convergence) {
	log.WithFields(log.Fields{
		"address": peer,
	}).Debug("Peer appeared")

	peerReceiver, ok := peer.(cla.ConvergenceSender)
	if !ok {
		log.Warn("Peer was not a ConvergenceSender")
		return
	}

	peerID := peerReceiver.GetPeerEndpointID()

	log.WithFields(log.Fields{
		"peer": peerID,
	}).Debug("PeerID discovered")

	dtlsr.dataMutex.Lock()
	defer dtlsr.dataMutex.Unlock()
	// track node
	dtlsr.newNode(peerID)

	// add node to peer list
	dtlsr.peers.Peers[peerID] = 0
	dtlsr.peers.Timestamp = bpv7.DtnTimeNow()
	dtlsr.peerChange = true

	log.WithFields(log.Fields{
		"peer": peerID,
	}).Debug("Peer is now being tracked")
}

func (dtlsr *DTLSR) ReportPeerDisappeared(peer cla.Convergence) {
	log.WithFields(log.Fields{
		"address": peer,
	}).Debug("Peer disappeared")

	peerReceiver, ok := peer.(cla.ConvergenceSender)
	if !ok {
		log.Warn("Peer was not a ConvergenceSender")
		return
	}

	peerID := peerReceiver.GetPeerEndpointID()

	log.WithFields(log.Fields{
		"peer": peerID,
	}).Debug("PeerID discovered")

	dtlsr.dataMutex.Lock()
	defer dtlsr.dataMutex.Unlock()
	// set expiration timestamp for peer
	timestamp := bpv7.DtnTimeNow()
	dtlsr.peers.Peers[peerID] = timestamp
	dtlsr.peers.Timestamp = timestamp
	dtlsr.peerChange = true

	log.WithFields(log.Fields{
		"peer": peer,
	}).Debug("Peer timeout is now running")
}

// DispatchingAllowed allows the processing of all packages.
func (_ *DTLSR) DispatchingAllowed(_ BundleDescriptor) bool {
	// TODO: for future optimisation, we might track the timestamp of the last recomputation of the routing table
	// and only dispatch if it changed since the last time we tried.
	return true
}

// newNode adds a node to the index-mapping (if it was not previously tracked)
func (dtlsr *DTLSR) newNode(id bpv7.EndpointID) {
	log.WithFields(log.Fields{
		"NodeID": id,
	}).Debug("Tracking Node")
	_, present := dtlsr.nodeIndex[id]

	if present {
		log.WithFields(log.Fields{
			"NodeID": id,
		}).Debug("Node already tracked")
		// node is already tracked
		return
	}

	dtlsr.nodeIndex[id] = dtlsr.length
	dtlsr.indexNode = append(dtlsr.indexNode, id)
	dtlsr.length = dtlsr.length + 1
	log.WithFields(log.Fields{
		"NodeID": id,
	}).Debug("Added node to tracking store")
}

// computeRoutingTable finds shortest paths using dijkstra's algorithm
func (dtlsr *DTLSR) computeRoutingTable() {
	log.Debug("Recomputing routing table")

	currentTime := bpv7.DtnTimeNow()
	graph := dijkstra.NewGraph()

	// add vertices
	for i := 0; i < dtlsr.length; i++ {
		graph.AddVertex(i)
		// log node-index mapping for debug purposes
		log.WithFields(log.Fields{
			"index": i,
			"node":  dtlsr.indexNode[i],
		}).Debug("Node-index-mapping")
	}

	// add edges originating from this node
	for peer, timestamp := range dtlsr.peers.Peers {
		var edgeCost int64
		if timestamp == 0 {
			edgeCost = 0
		} else {
			edgeCost = int64(currentTime - timestamp)
		}

		if err := graph.AddArc(0, dtlsr.nodeIndex[peer], edgeCost); err != nil {
			log.WithFields(log.Fields{
				"reason": err.Error(),
			}).Warn("Error computing routing table")
			return
		}

		log.WithFields(log.Fields{
			"peerA": dtlsr.c.NodeId,
			"peerB": peer,
			"cost":  edgeCost,
		}).Debug("Added vertex")
	}

	// add edges originating from other nodes
	for _, data := range dtlsr.receivedData {
		for peer, timestamp := range data.Peers {
			var edgeCost int64
			if timestamp == 0 {
				edgeCost = 0
			} else {
				edgeCost = int64(currentTime - timestamp)
			}

			if err := graph.AddArc(dtlsr.nodeIndex[data.ID], dtlsr.nodeIndex[peer], edgeCost); err != nil {
				log.WithFields(log.Fields{
					"reason": err.Error(),
				}).Warn("Error computing routing table")
				return
			}

			log.WithFields(log.Fields{
				"peerA": data.ID,
				"peerB": peer,
				"cost":  edgeCost,
			}).Debug("Added vertex")
		}
	}

	routingTable := make(map[bpv7.EndpointID]bpv7.EndpointID)
	for i := 1; i < dtlsr.length; i++ {
		shortest, err := graph.Shortest(0, i)
		if err == nil {
			if len(shortest.Path) <= 1 {
				log.WithFields(log.Fields{
					"node_index": i,
					"node":       dtlsr.indexNode[i],
					"path":       shortest.Path,
				}).Warn("Single step path found - this should not happen")
				continue
			}

			routingTable[dtlsr.indexNode[i]] = dtlsr.indexNode[shortest.Path[1]]
			log.WithFields(log.Fields{
				"node_index": i,
				"node":       dtlsr.indexNode[i],
				"path":       shortest.Path,
				"next_hop":   routingTable[dtlsr.indexNode[i]],
			}).Debug("Found path to node")
		} else {
			log.WithFields(log.Fields{
				"node_index": i,
				"error":      err.Error(),
			}).Debug("Did not find path to node")
		}
	}

	log.WithFields(log.Fields{
		"routingTable": routingTable,
	}).Debug("Finished routing table computation")

	dtlsr.routingTable = routingTable
}

// recomputeCron gets called periodically by the routing's cron module.
// Only actually triggers a recompute if the underlying data has changed.
func (dtlsr *DTLSR) recomputeCron() {
	dtlsr.dataMutex.RLock()
	peerChange := dtlsr.peerChange
	receivedChange := dtlsr.receivedChange
	dtlsr.dataMutex.RUnlock()

	log.WithFields(log.Fields{
		"peerChange":     peerChange,
		"receivedChange": receivedChange,
	}).Debug("Executing recomputeCron")

	if peerChange || receivedChange {
		dtlsr.dataMutex.Lock()
		dtlsr.computeRoutingTable()
		dtlsr.receivedChange = false
		dtlsr.dataMutex.Unlock()
	}
}

// broadcast broadcasts this node's peer data to the network
func (dtlsr *DTLSR) broadcast() {
	log.Debug("Broadcasting metadata")

	dtlsr.dataMutex.RLock()
	source := dtlsr.c.NodeId
	destination := dtlsr.broadcastAddress
	metadataBlock := bpv7.NewDTLSRBlock(dtlsr.peers)
	dtlsr.dataMutex.RUnlock()

	err := sendMetadataBundle(dtlsr.c, source, destination, metadataBlock)
	if err != nil {
		log.WithFields(log.Fields{
			"reason": err.Error(),
		}).Warn("Unable to send metadata")
	}
}

// broadcastCron gets called periodically by the routing's cron module.
// Only actually triggers a broadcast if peer data has changed
func (dtlsr *DTLSR) broadcastCron() {
	dtlsr.dataMutex.RLock()
	peerChange := dtlsr.peerChange
	dtlsr.dataMutex.RUnlock()

	log.WithFields(log.Fields{
		"peerChange": peerChange,
	}).Debug("Executing broadcastCron")

	if peerChange {
		dtlsr.broadcast()

		dtlsr.dataMutex.Lock()
		dtlsr.peerChange = false
		// a change in our own peer data should also trigger a routing recompute
		// but if this method gets called before recomputeCron(),
		// we don't want this information to be lost
		dtlsr.receivedChange = true
		dtlsr.dataMutex.Unlock()
	}
}

// purgePeers removes peers who have not been seen for a long time
func (dtlsr *DTLSR) purgePeers() {
	log.Debug("Executing purgePeers")
	currentTime := time.Now()

	dtlsr.dataMutex.Lock()
	defer dtlsr.dataMutex.Unlock()

	for peerID, timestamp := range dtlsr.peers.Peers {
		if timestamp != 0 && timestamp.Time().Add(dtlsr.purgeTime).Before(currentTime) {
			log.WithFields(log.Fields{
				"peer":            peerID,
				"disconnect_time": timestamp,
			}).Debug("Removing stale peer")
			delete(dtlsr.peers.Peers, peerID)
			dtlsr.peerChange = true
		}
	}
}
