package core

import (
	"fmt"
	"github.com/dtn7/cboring"
	"github.com/dtn7/dtn7-go/bundle"
	"github.com/dtn7/dtn7-go/cla"
	log "github.com/sirupsen/logrus"
	"gonum.org/v1/gonum/mat"
	"io"
	"sync"
)

type SimBetConfig struct {
	// Alpha + Beta = 1
	// Alpha is the weight of SimUtil
	Alpha float64
	// Beta is the weight of BetUtil
	Beta float64
}

type SimBet struct {
	c *Core
	// config contains the values for the tunable parameters
	config SimBetConfig
	// dataMutex is a RW-mutex which protects change operations to the algorithm's metadata
	dataMutex sync.RWMutex
	// nodeIndex and index Node are a bidirectional mapping EndpointID <-> uint64
	// when transforming the adjacency lists, this is ued to map EndpointIDs to matrix indices
	nodeIndex map[bundle.EndpointID]uint64
	length    uint64
	// adjacencies is the adjacency-list for nodes connected to this one
	// uses map instead of slice to ensure uniqueness of elements
	adjacencies map[bundle.EndpointID]bool
	// peerAdjacencies contains other node's adjacency lists, which we have received via an encounter
	peerAdjacencies map[bundle.EndpointID][]bundle.EndpointID
	// similarities contains this node's similarity values for all other nodes
	similarities map[bundle.EndpointID]float64
	// betweenness is this node's betweenness value
	betweenness float64
	// peerSimilarities contains the similarities-lists of all encountered nodes
	peerSimilarities map[bundle.EndpointID]map[bundle.EndpointID]float64
	// peerBetweenness contains the betweenness-values of all encountered nodes
	peerBetweenness map[bundle.EndpointID]float64
}

func NewSimBet(c *Core, config SimBetConfig) *SimBet {
	log.Debug("Initialising SimBet routing")
	simBet := SimBet{
		c:                c,
		config:           config,
		nodeIndex:        map[bundle.EndpointID]uint64{c.NodeId: 0},
		length:           1,
		adjacencies:      make(map[bundle.EndpointID]bool),
		peerAdjacencies:  make(map[bundle.EndpointID][]bundle.EndpointID),
		similarities:     make(map[bundle.EndpointID]float64),
		betweenness:      0.0,
		peerSimilarities: make(map[bundle.EndpointID]map[bundle.EndpointID]float64),
		peerBetweenness:  make(map[bundle.EndpointID]float64),
	}

	// register our custom metadata-block
	extensionBlockManager := bundle.GetExtensionBlockManager()
	if !extensionBlockManager.IsKnown(ExtBlockTypeAdjacencyBlock) {
		// since we already checked if the block type exists, this really shouldn't ever fail...
		_ = extensionBlockManager.Register(NewAdjacencyBlock(simBet.adjacencies))
	}
	if !extensionBlockManager.IsKnown(ExtBlockTypeSimBetSummaryVector) {
		// since we already checked if the block type exists, this really shouldn't ever fail...
		_ = extensionBlockManager.Register(NewSimBetSummaryVector(simBet.betweenness, simBet.similarities))
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

		for i := 0; i < len(adjacencies); i++ {
			simBet.trackNode(adjacencies[i])
		}

		simBet.dataMutex.Lock()
		simBet.peerAdjacencies[peerID] = adjacencies
		simBet.dataMutex.Unlock()

		log.WithFields(log.Fields{
			"bundle": bp.ID(),
			"peer":   peerID,
		}).Debug("Saved peer metadata")

		simBet.updateBetweenness()
		simBet.updateSimilarity()

		return
	}

	bndlItem, err := simBet.c.store.QueryId(bndl.ID())
	if err != nil {
		log.WithFields(log.Fields{
			"bundle": bndl.ID(),
			"error":  err.Error(),
		}).Warn("Unable to get BundleItem")
		return
	}

	bndlItem.Properties["destination"] = bndl.PrimaryBlock.Destination

	if err = simBet.c.store.Update(bndlItem); err != nil {
		log.WithFields(log.Fields{
			"bundle": bndl.ID(),
			"error":  err.Error(),
		}).Warn("Unable to save BundleItem")
		return
	}
}

func (simBet *SimBet) Fatal(fields log.Fields, message string) {
	if fields == nil {
		log.Fatal(fmt.Sprintf("SimBet: %s", message))
	} else {
		log.WithFields(fields).Fatal(fmt.Sprintf("SimBet: %s", message))
	}
}

func (simBet *SimBet) Warn(fields log.Fields, message string) {
	if fields == nil {
		log.Warn(fmt.Sprintf("SimBet: %s", message))
	} else {
		log.WithFields(fields).Warn(fmt.Sprintf("SimBet: %s", message))
	}
}

func (simBet *SimBet) Debug(fields log.Fields, message string) {
	if fields == nil {
		log.Debug(fmt.Sprintf("SimBet: %s", message))
	} else {
		log.WithFields(fields).Debug(fmt.Sprintf("SimBet: %s", message))
	}
}

func (simBet *SimBet) Info(fields log.Fields, message string) {
	if fields == nil {
		log.Info(fmt.Sprintf("SimBet: %s", message))
	} else {
		log.WithFields(fields).Info(fmt.Sprintf("SimBet: %s", message))
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

	simBet.trackNode(peerID)

	simBet.dataMutex.Lock()
	simBet.adjacencies[peerID] = true
	simBet.dataMutex.Unlock()

	log.WithFields(log.Fields{
		"peer": peerID,
	}).Debug("Peer added to adjacencies")

	simBet.sendAdjacencies(peerID)
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
func (simBet *SimBet) sendAdjacencies(peerID bundle.EndpointID) {
	simBet.dataMutex.RLock()
	source := simBet.c.NodeId
	metadataBlock := NewAdjacencyBlock(simBet.adjacencies)
	simBet.dataMutex.RUnlock()

	err := sendMetadataBundle(simBet.c, source, peerID, metadataBlock)
	if err != nil {
		log.WithFields(log.Fields{
			"peer":   peerID,
			"reason": err.Error(),
		}).Warn("Error sending adjacencies")
	}
}

func (simBet *SimBet) sendSummaryVector(peerID bundle.EndpointID) {
	// filter similarities. we only want the values for nodes for which we are actually carrying messages
	filteredSimilarities := make(map[bundle.EndpointID]float64)
	pendingBundles, err := simBet.c.store.QueryPending()
	if err != nil {
		log.WithFields(log.Fields{
			"peer":  peerID,
			"error": err.Error(),
		}).Warn("Failed to get pending bundles")
	}
	nBndl := len(pendingBundles)

	simBet.dataMutex.RLock()
	for i := 0; i < nBndl; i++ {
		bndlItem := pendingBundles[i]
		destination, ok := bndlItem.Properties["destination"].(bundle.EndpointID)
		if !ok {
			log.WithFields(log.Fields{
				"bundle": bndlItem.Id,
			}).Warn("Unable to get bundle destination")
			continue
		}
		filteredSimilarities[destination] = simBet.similarities[destination]
	}
	simBet.dataMutex.RUnlock()

	summaryVector := NewSimBetSummaryVector(simBet.betweenness, filteredSimilarities)

	err = sendMetadataBundle(simBet.c, simBet.c.NodeId, peerID, summaryVector)
	if err != nil {
		log.WithFields(log.Fields{
			"peer":   peerID,
			"reason": err.Error(),
		}).Warn("Error sending summary vector")
	}
}

func (simBet *SimBet) updateSimilarity() {
	// TODO: Dummy Implementation
}

func (simBet *SimBet) updateBetweenness() {
	log.Debug("Building adjacency matrix")
	// initialise matrix
	adjacencyMatrix := mat.NewDense(int(simBet.length), int(simBet.length), nil)
	adjacencyMatrix.Zero()

	simBet.dataMutex.RLock()
	// add our own adjacencies
	mapSlice := adjacencyMapToSlice(simBet.adjacencies)
	row := make([]float64, simBet.length)
	for i := 0; i < len(mapSlice); i++ {
		peerID := mapSlice[i]
		index := simBet.nodeIndex[peerID]
		row[index] = 1
	}
	adjacencyMatrix.SetRow(1, row)

	for rowPeer, adjacencies := range simBet.peerAdjacencies {
		rowIndex := simBet.nodeIndex[rowPeer]
		row = make([]float64, simBet.length)
		for i := 0; i < len(adjacencies); i++ {
			peerID := adjacencies[i]
			index := simBet.nodeIndex[peerID]
			row[index] = 1
		}
		adjacencyMatrix.SetRow(int(rowIndex), row)
	}
	simBet.dataMutex.RUnlock()
	log.WithFields(log.Fields{
		"matrix": adjacencyMatrix,
	}).Debug("Finished building adjacency matrix")

	var aSquared mat.Dense
	aSquared.Mul(adjacencyMatrix, adjacencyMatrix)
	simBet.Debug(log.Fields{
		"matrix": aSquared,
	}, "Squared matrix")

	xDim, yDim := aSquared.Dims()
	ones := make([]float64, xDim * yDim)
	for i, _ := range ones {
		ones[i] = 1.0
	}
	onesMatrix := mat.NewDense(xDim, yDim, ones)

	var maskMatrix mat.Dense
	maskMatrix.Sub(onesMatrix, adjacencyMatrix)
	simBet.Debug(log.Fields{
		"matrix": maskMatrix,
	}, "Binary mask matrix")

	// iterate over top right half of matrix
	// only take those values from aSquared where the maskMatrix has a 1
	// add all the reciprocals of these values
	betweenness := 0.0
	for xIndex := 0; xIndex < xDim; xIndex++ {
		for yIndex := xIndex + 1; yIndex < yDim; yIndex++ {
			if maskMatrix.At(xIndex, yIndex) > 0.5 {
				betweenness += 1 / aSquared.At(xIndex, yIndex)
			}
		}
	}

	simBet.Debug(log.Fields{
		"betweenness": betweenness,
	}, "Final betweenness")

	simBet.dataMutex.Lock()
	simBet.betweenness = betweenness
	simBet.dataMutex.Unlock()
}

func (simBet *SimBet) computeSimUtil(otherNode bundle.EndpointID, destination bundle.EndpointID) float64 {
	similarity := simBet.similarities[destination]
	return similarity / (similarity + simBet.peerSimilarities[otherNode][destination])
}

func (simBet *SimBet) computeBetUtil(otherNode bundle.EndpointID) float64 {
	return simBet.betweenness / (simBet.betweenness + simBet.peerBetweenness[otherNode])
}

func (simBet *SimBet) computeSimBetUtil(otherNode bundle.EndpointID, desitnation bundle.EndpointID) float64 {
	return (simBet.config.Alpha * simBet.computeSimUtil(otherNode, desitnation)) + (simBet.config.Beta * simBet.computeBetUtil(otherNode))
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

// track node adds a node's id to the nodeIndex-map
func (simBet *SimBet) trackNode(id bundle.EndpointID) {
	log.WithFields(log.Fields{
		"peerID": id,
	}).Debug("SIMBET: Looking to track node")
	simBet.dataMutex.Lock()
	defer simBet.dataMutex.Unlock()

	_, present := simBet.nodeIndex[id]
	if present {
		log.WithFields(log.Fields{
			"peerID": id,
		}).Debug("SIMBET: Node already tracked")
		// the node id is already being tracked and we don't need to do anything
		return
	}

	simBet.nodeIndex[id] = simBet.length
	simBet.length++

	log.WithFields(log.Fields{
		"peerID": id,
	}).Debug("SIMBET: Node is now being tracked")
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

const ExtBlockTypeSimBetSummaryVector uint64 = 196

// SimBetSummaryVector contains the node's betweenness and similarity values
// for all nodes which are recipients of carried bundles
type SimBetSummaryVector struct {
	Betweenness  float64
	Similarities map[bundle.EndpointID]float64
}

func NewSimBetSummaryVector(betweenness float64, similarities map[bundle.EndpointID]float64) *SimBetSummaryVector {
	summaryVector := SimBetSummaryVector{
		Betweenness:  betweenness,
		Similarities: similarities,
	}
	return &summaryVector
}

func (summaryVector *SimBetSummaryVector) BlockTypeCode() uint64 {
	return ExtBlockTypeSimBetSummaryVector
}

func (summaryVector *SimBetSummaryVector) CheckValid() error {
	return nil
}

func (summaryVector *SimBetSummaryVector) MarshalCbor(w io.Writer) error {
	// write struct ''length''
	if err := cboring.WriteArrayLength(2, w); err != nil {
		return err
	}

	// write betweenness
	if err := cboring.WriteFloat64(summaryVector.Betweenness, w); err != nil {
		return err
	}

	if err := cboring.WriteMapPairLength(uint64(len(summaryVector.Similarities)), w); err != nil {
		return err
	}

	for peerID, similarity := range summaryVector.Similarities {
		if err := cboring.Marshal(&peerID, w); err != nil {
			return err
		}
		if err := cboring.WriteFloat64(similarity, w); err != nil {
			return err
		}
	}

	return nil
}

func (summaryVector *SimBetSummaryVector) UnmarshalCbor(r io.Reader) error {
	lenStruct, err := cboring.ReadArrayLength(r)
	if err != nil {
		return err
	} else if lenStruct != 2 {
		return fmt.Errorf("expected 2 fields, got %d", lenStruct)
	}

	betweenness, err := cboring.ReadFloat64(r)
	if err != nil {
		return err
	}
	summaryVector.Betweenness = betweenness

	simLen, err := cboring.ReadMapPairLength(r)
	if err != nil {
		return err
	}

	similarities := make(map[bundle.EndpointID]float64, simLen)
	for ; simLen > 0; simLen-- {
		peerID := bundle.EndpointID{}
		if err := cboring.Unmarshal(&peerID, r); err != nil {
			return err
		}
		similarity, err := cboring.ReadFloat64(r)
		if err != nil {
			return err
		}
		similarities[peerID] = similarity
	}
	summaryVector.Similarities = similarities

	return nil
}
