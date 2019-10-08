package core

import (
	"github.com/dtn7/cboring"
	"github.com/dtn7/dtn7-go/bundle"
	"github.com/dtn7/dtn7-go/cla"
	log "github.com/sirupsen/logrus"
	"io"
	"sync"
)

type SimBet struct {
	c *Core
	// dataMutex is a RW-mutex which protects change operations to the algorithm's metadata
	dataMutex sync.RWMutex
	// adjacencies is the adjacency-list for nodes connected to this one
	// uses map instead of slice to ensure uniqueness of elements
	adjacencies map[bundle.EndpointID]bool
	// peerAdjacencies contains other node's adjacency lists, which we have received via an encounter
	peerAdjacencies map[bundle.EndpointID][]bundle.EndpointID
}

func NewSimBet(c *Core) *SimBet {
	log.Debug("Initialised SimBet routing")
	simBet := SimBet{
		c:               c,
		adjacencies:     make(map[bundle.EndpointID]bool),
		peerAdjacencies: make(map[bundle.EndpointID][]bundle.EndpointID),
	}

	// register our custom metadata-block
	extensionBlockManager := bundle.GetExtensionBlockManager()
	if !extensionBlockManager.IsKnown(ExtBlockTypeAdjacencyBlock) {
		// since we already checked if the block type exists, this really shouldn't ever fail...
		_ = extensionBlockManager.Register(NewAdjacencyBlock(simBet.adjacencies))
	}

	return &simBet
}

func (simBet *SimBet) NotifyIncoming(bp BundlePack) {
	bndl, err := bp.Bundle()
	if err != nil {
		log.WithFields(log.Fields{
			"bundle": bp.ID(),
			"error":  err.Error(),
		}).Warn("Unable to load bundle data")
		return
	}

	peerID := bndl.PrimaryBlock.SourceNode
	if peerID == simBet.c.NodeId {
		// this is data we have generated ourselves
		return
	}

	if metaDataBlock, err := bndl.ExtensionBlock(ExtBlockTypeAdjacencyBlock); err == nil {
		log.WithFields(log.Fields{
			"bundle": bp.ID(),
			"peer":   peerID,
		}).Debug("Received metadata")

		adjacencyBlock := metaDataBlock.Value.(*AdjacencyBlock)
		adjacencies := adjacencyBlock.getAdjacencies()
		log.WithFields(log.Fields{
			"bundle":      bp.ID(),
			"peer":        peerID,
			"adjacencies": adjacencies,
		}).Debug("Parsed Metadata")

		simBet.dataMutex.Lock()
		simBet.peerAdjacencies[peerID] = adjacencies
		simBet.dataMutex.Unlock()

		log.WithFields(log.Fields{
			"bundle": bp.ID(),
			"peer":   peerID,
		}).Debug("Saved peer metadata")
	}
}

func (simBet *SimBet) DispatchingAllowed(bp BundlePack) bool {
	// TODO: Dummy Implementation
	return true
}

func (simBet *SimBet) SenderForBundle(bp BundlePack) (sender []cla.ConvergenceSender, delete bool) {
	// TODO: Dummy Implementation
	return nil, false
}

func (simBet *SimBet) ReportFailure(bp BundlePack, sender cla.ConvergenceSender) {
	// TODO: Dummy Implementation
}

func (simBet *SimBet) ReportPeerAppeared(peer cla.Convergence) {
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

	simBet.dataMutex.Lock()
	simBet.adjacencies[peerID] = true
	simBet.dataMutex.Unlock()

	log.WithFields(log.Fields{
		"peer": peerID,
	}).Debug("Peer added to adjacencies")

	simBet.sendMetadata(peerID)
}

func (simBet *SimBet) ReportPeerDisappeared(peer cla.Convergence) {
	log.WithFields(log.Fields{
		"address": peer,
	}).Debug("Peer disappeared")

	peerReceiver, ok := peer.(cla.ConvergenceSender)
	if !ok {
		log.Debug("Peer was not a ConvergenceSender")
		return
	}

	peerID := peerReceiver.GetPeerEndpointID()

	log.WithFields(log.Fields{
		"peer": peerID,
	}).Debug("PeerID discovered")

	simBet.dataMutex.Lock()
	defer simBet.dataMutex.Unlock()

	delete(simBet.adjacencies, peerID)

	log.WithFields(log.Fields{
		"peer": peerID,
	}).Debug("Peer removed from adjacencies")
}

// sendAdjacencies sends our list of adjacencies to the connected peer
// TODO: This is very similar to the sendMetadata functions of prophet/dtlsr
func (simBet *SimBet) sendMetadata(peerId bundle.EndpointID) {
	bundleBuilder := bundle.Builder()
	bundleBuilder.Destination(peerId)
	bundleBuilder.CreationTimestampNow()
	bundleBuilder.Lifetime("10m")
	bundleBuilder.BundleCtrlFlags(bundle.MustNotFragmented)
	// no Payload
	bundleBuilder.PayloadBlock(byte(1))

	simBet.dataMutex.RLock()

	bundleBuilder.Source(simBet.c.NodeId)
	metadataBlock := NewAdjacencyBlock(simBet.adjacencies)

	simBet.dataMutex.RUnlock()

	bundleBuilder.Canonical(metadataBlock)
	metadataBundle, err := bundleBuilder.Build()
	if err != nil {
		log.WithFields(log.Fields{
			"reason": err.Error(),
		}).Warn("Unable to build metadata bundle")
		return
	} else {
		log.Debug("Metadata Bundle built")
	}

	log.Debug("Sending metadata bundle")
	simBet.c.SendBundle(&metadataBundle)
	log.WithFields(log.Fields{
		"bundle": metadataBundle,
	}).Debug("Successfully sent metadata bundle")
}

// adjacencyMapToSlice transforms the map-structure which is used to track adjacencies efficiently
// into a slice which can be sent over the network more efficiently
func adjacencyMapToSlice(adjacencies map[bundle.EndpointID]bool) []bundle.EndpointID {
	adjSlice := make([]bundle.EndpointID, len(adjacencies))

	i := 0
	for k := range adjacencies {
		adjSlice[i] = k
		i++
	}

	return adjSlice
}

const ExtBlockTypeAdjacencyBlock uint64 = 195

type AdjacencyBlock []bundle.EndpointID

func NewAdjacencyBlock(adjacencies map[bundle.EndpointID]bool) *AdjacencyBlock {
	adjacencyBlock := AdjacencyBlock(adjacencyMapToSlice(adjacencies))
	return &adjacencyBlock
}

func (adjacencyBlock *AdjacencyBlock) getAdjacencies() []bundle.EndpointID {
	return *adjacencyBlock
}

func (adjacencyBlock *AdjacencyBlock) BlockTypeCode() uint64 {
	return ExtBlockTypeAdjacencyBlock
}

func (adjacencyBlock *AdjacencyBlock) CheckValid() error {
	return nil
}

func (adjacencyBlock *AdjacencyBlock) MarshalCbor(w io.Writer) error {
	if err := cboring.WriteArrayLength(uint64(len(*adjacencyBlock)), w); err != nil {
		return err
	}

	for _, peerID := range *adjacencyBlock {
		if err := cboring.Marshal(&peerID, w); err != nil {
			return err
		}
	}

	return nil
}

func (adjacencyBlock *AdjacencyBlock) UnmarshalCbor(r io.Reader) error {
	var length uint64
	length, err := cboring.ReadArrayLength(r)
	if err != nil {
		return err
	}

	adjacencies := make([]bundle.EndpointID, length)
	var i uint64
	var peerID bundle.EndpointID
	for i = 0; i < length; i++ {
		peerID = bundle.EndpointID{}
		if err := cboring.Unmarshal(&peerID, r); err != nil {
			return err
		}
		adjacencies[i] = peerID
	}

	*adjacencyBlock = adjacencies

	return nil
}
