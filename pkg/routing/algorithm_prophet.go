// SPDX-FileCopyrightText: 2019, 2021 Markus Sommer
// SPDX-FileCopyrightText: 2020 Alvar Penning
//
// SPDX-License-Identifier: GPL-3.0-or-later

package routing

import (
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/dtn7/dtn7-go/pkg/bpv7"
	"github.com/dtn7/dtn7-go/pkg/cla"
)

type ProphetConfig struct {
	// PInit ist the prophet initialisation constant
	PInit float64
	// Beta is the prophet scaling factor for transitive predictability
	Beta float64
	// Gamma is the prophet ageing factor
	Gamma float64
	// AgeInterval is the duration after which entries are aged
	AgeInterval string
}

type Prophet struct {
	c *Core
	// predictabilities are this node's delivery probabilities for other nodes
	predictabilities map[bpv7.EndpointID]float64
	// Map containing the predictability-maps of other nodes
	peerPredictabilities map[bpv7.EndpointID]map[bpv7.EndpointID]float64
	// dataMutex is a RW-mutex which protects change operations to the algorithm's metadata
	dataMutex sync.RWMutex
	// config contains the values for prophet constants
	config ProphetConfig
}

func NewProphet(c *Core, config ProphetConfig) *Prophet {
	log.WithFields(log.Fields{
		"p_init":       config.PInit,
		"beta":         config.Beta,
		"gamma":        config.Gamma,
		"age_interval": config.AgeInterval,
	}).Info("Initialised Prophet")

	prophet := Prophet{
		c:                    c,
		predictabilities:     make(map[bpv7.EndpointID]float64),
		peerPredictabilities: make(map[bpv7.EndpointID]map[bpv7.EndpointID]float64),
		config:               config,
	}

	ageInterval, err := time.ParseDuration(config.AgeInterval)
	if err != nil {
		log.WithFields(log.Fields{
			"string": config.AgeInterval,
		}).Fatal("Unable to parse duration")
	}

	err = c.cron.Register("dtlsr_recompute", prophet.ageCron, ageInterval)
	if err != nil {
		log.WithFields(log.Fields{
			"reason": err.Error(),
		}).Warn("Could not register DTLSR recompute job")
	}

	// register our custom metadata-block
	extensionBlockManager := bpv7.GetExtensionBlockManager()
	if !extensionBlockManager.IsKnown(bpv7.ExtBlockTypeProphetBlock) {
		// since we already checked if the block type exists, this really shouldn't ever fail...
		_ = extensionBlockManager.Register(bpv7.NewProphetBlock(prophet.predictabilities))
	}

	return &prophet
}

// encounter updates the predictability for an encountered node
func (prophet *Prophet) encounter(peer bpv7.EndpointID) {
	// map will return 0 if no value is stored for key
	pOld := prophet.predictabilities[peer]
	pNew := pOld + ((1 - pOld) * prophet.config.PInit)
	prophet.predictabilities[peer] = pNew
	log.WithFields(log.Fields{
		"peer": peer,
		"pOld": pOld,
		"pNew": pNew,
	}).Debug("Updated predictability via encounter")
}

// agePred "ages" - decreases over time - the predictability for a node
func (prophet *Prophet) agePred(peer bpv7.EndpointID) {
	pOld := prophet.predictabilities[peer]
	pNew := pOld * prophet.config.Gamma
	prophet.predictabilities[peer] = pNew
	log.WithFields(log.Fields{
		"peer": peer,
		"pOld": pOld,
		"pNew": pNew,
	}).Debug("Updated predictability via ageing")
}

// ageCron gets called periodically by the routing's cron ange ages all peer predictabilities
func (prophet *Prophet) ageCron() {
	prophet.dataMutex.Lock()
	defer prophet.dataMutex.Unlock()
	for peer := range prophet.predictabilities {
		prophet.agePred(peer)
	}
}

// transitivity increases predictability for nodes based on a peer's corresponding predictability
// If we are likely to reencounter node b and node b is likely to reencounter node c
// then we are also a good forwarder for node c
func (prophet *Prophet) transitivity(peer bpv7.EndpointID) {
	// map will return 0 if no value is stored for key
	peerPredictabilities, present := prophet.peerPredictabilities[peer]
	if !present {
		log.WithFields(log.Fields{
			"peer": peer,
		}).Debug("Don't know peer's predictabilities")
		return
	}

	log.WithFields(log.Fields{
		"peer": peer,
	}).Debug("Updating transitive predictabilities")

	for otherPeer, otherPeerPred := range peerPredictabilities {
		// map will return 0 if no value is stored for key
		peerPred := prophet.predictabilities[peer]
		pOld := prophet.predictabilities[otherPeer]
		pNew := pOld + ((1 - pOld) * peerPred * otherPeerPred * prophet.config.Beta)
		prophet.predictabilities[otherPeer] = pNew
		log.WithFields(log.Fields{
			"beta":            prophet.config.Beta,
			"peer":            peer,
			"peer_pred":       peerPred,
			"other_peer":      otherPeer,
			"other_peer_pred": otherPeerPred,
			"pOld":            pOld,
			"pNew":            pNew,
		}).Debug("Updated predictability via transitivity")
	}
}

// sendMetadata sends our summary-vector with our delivery predictabilities to a peer
func (prophet *Prophet) sendMetadata(destination bpv7.EndpointID) {
	prophet.dataMutex.RLock()
	source := prophet.c.NodeId
	metadataBlock := bpv7.NewProphetBlock(prophet.predictabilities)
	prophet.dataMutex.RUnlock()

	err := sendMetadataBundle(prophet.c, source, destination, metadataBlock)

	if err != nil {
		log.WithFields(log.Fields{
			"peer":   destination,
			"reason": err.Error(),
		}).Warn("Unable to send metadata bundle")
		return
	}
}

func (prophet *Prophet) NotifyNewBundle(bp BundleDescriptor) {
	log.WithFields(log.Fields{
		"bundle": bp.ID(),
	}).Debug("Incoming bundle")

	if metaDataBlock, err := bp.MustBundle().ExtensionBlock(bpv7.ExtBlockTypeProphetBlock); err == nil {
		log.WithFields(log.Fields{
			"bundle": bp.ID(),
			"source": bp.MustBundle().PrimaryBlock.SourceNode,
		}).Debug("Received metadata")

		if bp.MustBundle().PrimaryBlock.Destination != prophet.c.NodeId {
			log.WithFields(log.Fields{
				"recipient": bp.MustBundle().PrimaryBlock.Destination,
				"own_id":    prophet.c.NodeId,
			}).Debug("Received Metadata meant for different node")
			return
		}

		prophetBlock := metaDataBlock.Value.(*bpv7.ProphetBlock)
		data := prophetBlock.GetPredictabilities()
		peerID := bp.MustBundle().PrimaryBlock.SourceNode

		log.WithFields(log.Fields{
			"source": bp.MustBundle().PrimaryBlock.SourceNode,
			"data":   data,
		}).Debug("Decoded peer data")

		prophet.dataMutex.Lock()
		defer prophet.dataMutex.Unlock()

		_, present := prophet.peerPredictabilities[peerID]
		if present {
			log.WithFields(log.Fields{
				"peer": peerID,
			}).Debug("Updating peer metadata")
		} else {
			log.WithFields(log.Fields{
				"peer": peerID,
			}).Debug("Metadata for new peer")
		}

		// import new metadata
		prophet.peerPredictabilities[peerID] = data

		// update own predictabilities via the transitive property
		prophet.transitivity(peerID)

		return
	}

	// handle non-metadata bundles
	// TODO: this is basically copy-pasted from routing_epidemic - extract this code into a reusable function

	bundleItem, err := prophet.c.store.QueryId(bp.Id)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Warn("Failed to proceed a non-stored Bundle")
		return
	}

	bndl, err := bp.Bundle()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Warn("Couldn't get bundle data")
		return
	}

	// Check if we got a PreviousNodeBlock and extract its EndpointID
	var prevNode bpv7.EndpointID
	if pnBlock, err := bndl.ExtensionBlock(bpv7.ExtBlockTypePreviousNodeBlock); err == nil {
		prevNode = pnBlock.Value.(*bpv7.PreviousNodeBlock).Endpoint()

		log.WithFields(log.Fields{
			"bundle": bndl.ID(),
			"dst": 	  bndl.PrimaryBlock.Destination.String(),
			"src":    prevNode,
		}).Info("Received bundle from peer")
	} else {
		return
	}

	sentEids, ok := bundleItem.Properties["routing/prophet/sent"].([]bpv7.EndpointID)
	if !ok {
		sentEids = make([]bpv7.EndpointID, 0)
	}

	// Check if PreviousNodeBlock is already known
	for _, eids := range sentEids {
		if eids == prevNode {
			return
		}
	}

	log.WithFields(log.Fields{
		"bundle": bp.ID(),
		"eid":    prevNode,
	}).Debug("Prophet received an incomming bundle and checked its PreviousNodeBlock")

	bundleItem.Properties["routing/prophet/sent"] = append(sentEids, prevNode)
	if err := prophet.c.store.Update(bundleItem); err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Warn("Updating BundleItem failed")
	}
}

// TODO: dummy implementation
func (prophet *Prophet) DispatchingAllowed(_ BundleDescriptor) bool {
	return true
}

func (prophet *Prophet) SenderForBundle(bp BundleDescriptor) (sender []cla.ConvergenceSender, delete bool) {
	log.WithFields(log.Fields{
		"bundle": bp.ID(),
	}).Debug("Starting routing decision")

	defer log.WithFields(log.Fields{
		"bundle": bp.ID(),
	}).Debug("Routing decision finished")

	bndl, err := bp.Bundle()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Warn("Couldn't get bundle data")
		return
	}

	if _, err := bndl.ExtensionBlock(bpv7.ExtBlockTypeProphetBlock); err == nil {
		// we do not forward metadata bundles
		// if the intended recipient is connected the bundle will be forwarded via direct delivery
		// since we shouldn't have any metadata bundle meant for other nodes, we will also delete these bundles
		// if we find them in our store
		return nil, true
	}

	delete = false

	bundleItem, err := prophet.c.store.QueryId(bp.Id)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Warn("Failed to proceed a non-stored Bundle")
		return
	}

	sentEids, ok := bundleItem.Properties["routing/prophet/sent"].([]bpv7.EndpointID)
	if !ok {
		sentEids = make([]bpv7.EndpointID, 0)
	}

	destination := bndl.PrimaryBlock.Destination
	sender = make([]cla.ConvergenceSender, 0)

	for _, cs := range prophet.c.claManager.Sender() {
		peerID := cs.GetPeerEndpointID()
		peerPred := prophet.peerPredictabilities[peerID][destination]
		ownPred := prophet.predictabilities[destination]

		// is the peers delivery predictability for the destination greater than ours?
		if peerPred > ownPred {
			// TODO: this is again very similar to epidemic - could we put that in a function as well?

			log.WithFields(log.Fields{
				"bundle":      bndl.ID(),
				"destination": destination,
				"peer":        peerID,
				"ownPred":     ownPred,
				"peerPred":    peerPred,
			}).Debug("Found possible forwarding candidate")

			skip := false
			for _, eid := range sentEids {
				if peerID == eid {
					skip = true
					log.WithFields(log.Fields{
						"bundle": bndl.ID(),
						"peer":   peerID,
					}).Debug("Peer already has this bundle")
					break
				}
			}

			if !skip {
				sender = append(sender, cs)
				sentEids = append(sentEids, peerID)
				log.WithFields(log.Fields{
					"bundle": bndl.ID(),
					"peer":   peerID,
				}).Debug("Will forward bundle to peer.")
			}
		} else {
			log.WithFields(log.Fields{
				"bundle":      bndl.ID(),
				"destination": destination,
				"peer":        peerID,
				"ownPred":     ownPred,
				"peerPred":    peerPred,
			}).Debug("Peer is not good forwarding candidate")
		}
	}

	if len(sender) == 0 {
		log.WithFields(
			log.Fields{
				"bundle": bndl.ID(),
			}).Debug("Did not find peer to forward to")
		return
	}

	bundleItem.Properties["routing/prophet/sent"] = sentEids
	if err := prophet.c.store.Update(bundleItem); err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Warn("Updating BundleItem failed")
	}

	log.WithFields(log.Fields{
		"bundle":              bndl.ID(),
		"sent":                sentEids,
		"convergence-senders": sender,
	}).Debug("Prophet selected Convergence Senders for an outgoing bundle")

	return
}

func (prophet *Prophet) ReportFailure(bp BundleDescriptor, sender cla.ConvergenceSender) {
	bundleItem, err := prophet.c.store.QueryId(bp.Id)
	if err != nil {
		log.WithFields(log.Fields{
			"bundle": bp.ID(),
			"error":  err.Error(),
		}).Warn("Failed to get bundle metadata")
		return
	}

	sentEids, ok := bundleItem.Properties["routing/prophet/sent"].([]bpv7.EndpointID)
	if !ok {
		// this shouldn't really happen, no?
		log.WithFields(log.Fields{
			"bundle": bp.ID(),
		}).Warn("Bundle had no stored sender-list")
		return
	}

	log.WithFields(log.Fields{
		"bundle": bp.ID(),
		"peer":   sender,
	}).Info("Failed to transmit bundle")

	for i := 0; i < len(sentEids); i++ {
		if sentEids[i] == sender.GetPeerEndpointID() {
			sentEids = append(sentEids[:i], sentEids[i+1:]...)
			break
		}
	}

	bundleItem.Properties["routing/prophet/sent"] = sentEids

	if err := prophet.c.store.Update(bundleItem); err != nil {
		log.WithFields(log.Fields{
			"bundle": bp.ID(),
			"error":  err,
		}).Warn("Updating BundleItem failed")
		return
	}

	log.WithFields(log.Fields{
		"bundle": bp.ID(),
		"peer":   sender,
		"clas":   sentEids,
	}).Debug("Removed peer from sent list")
}

func (prophet *Prophet) ReportPeerAppeared(peer cla.Convergence) {
	log.WithFields(log.Fields{
		"address": peer,
	}).Debug("Peer appeared")

	peerReceiver, ok := peer.(cla.ConvergenceSender)
	if !ok {
		log.Debug("Peer was not a ConvergenceSender")
		return
	}

	peerID := peerReceiver.GetPeerEndpointID()

	log.WithFields(log.Fields{
		"peer": peerID,
	}).Debug("PeerID discovered")

	// update our delivery predictability for this peer
	prophet.dataMutex.Lock()
	prophet.encounter(peerID)
	prophet.dataMutex.Unlock()

	// send them our summary vector
	prophet.sendMetadata(peerID)
}

func (prophet *Prophet) ReportPeerDisappeared(peer cla.Convergence) {
	log.WithFields(log.Fields{
		"address": peer,
	}).Debug("Peer disappeared")
	// there really isn't anything to do upon a peer's disappearance
}
