package core

import (
	"io"

	"github.com/dtn7/cboring"
	"github.com/dtn7/dtn7-go/bundle"
	"github.com/dtn7/dtn7-go/cla"
	log "github.com/sirupsen/logrus"
)

type Rapid struct {
	c *Core
}

func NewRapid(c *Core) *Rapid {
	rapid := Rapid{
		c: c,
	}

	log.Info("Initialised Rapid routing")

	return &rapid
}

func (rapid *Rapid) NotifyIncoming(bp BundlePack) {
	// TODO: Dummy Implementation
}

func (rapid *Rapid) DispatchingAllowed(bp BundlePack) bool {
	// TODO: Dummy Implementation
	return true
}

func (rapid *Rapid) SenderForBundle(bp BundlePack) (sender []cla.ConvergenceSender, delete bool) {
	// TODO: Dummy Implementation
	return nil, false
}

func (rapid *Rapid) ReportFailure(bp BundlePack, sender cla.ConvergenceSender) {
	// TODO: Dummy Implementation
}

func (rapid *Rapid) ReportPeerAppeared(peer cla.Convergence) {
	log.WithFields(log.Fields{
		"address": peer,
	}).Debug("RAPID: Peer appeared")

	peerReceiver, ok := peer.(cla.ConvergenceSender)
	if !ok {
		log.Debug("RAPID: Peer was not a ConvergenceSender")
		return
	}

	peerID := peerReceiver.GetPeerEndpointID()

	log.WithFields(log.Fields{
		"peer": peerID,
	}).Debug("RAPID: PeerID discovered")

	// get the IDs of undelivered bundles
	bundleItems, err := rapid.c.store.QueryPending()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Warn("Unable to get pending bundles")
	}

	n := len(bundleItems)
	bundleIDs := make([]bundle.BundleID, n)
	for i := 0; i < n; i++ {
		bundleIDs[i] = bundleItems[i].BId
	}

	metadataBlock := NewRapidBlock(bundleIDs)

	err = sendMetadataBundle(rapid.c, rapid.c.NodeId, peerID, metadataBlock)
	if err != nil {
		log.WithFields(log.Fields{
			"peer":  peerID,
			"error": err.Error(),
		}).Warn("RAPID: Unable to send metadata bundle")
	}
}

func (rapid *Rapid) ReportPeerDisappeared(peer cla.Convergence) {
	// TODO: Dummy Implementation
}

const ExtBlockTypeRapidBlock uint64 = 196

type RapidBlock []bundle.BundleID

func NewRapidBlock(bunldes []bundle.BundleID) *RapidBlock {
	rapidBlock := RapidBlock(bunldes)
	return &rapidBlock
}

func (rapidBlock *RapidBlock) getBundles() []bundle.BundleID {
	return *rapidBlock
}

func (rapidBlock *RapidBlock) BlockTypeCode() uint64 {
	return ExtBlockTypeRapidBlock
}

func (rapidBlock *RapidBlock) CheckValid() error {
	return nil
}

func (rapidBlock *RapidBlock) MarshalCbor(w io.Writer) error {
	if err := cboring.WriteArrayLength(uint64(len(*rapidBlock)), w); err != nil {
		return err
	}
	for i := range *rapidBlock {
		bundleID := (*rapidBlock)[i]
		if err := cboring.Marshal(&bundleID, w); err != nil {

		}
	}

	return nil
}

func (rapidBlock *RapidBlock) UnmarshalCbor(r io.Reader) error {
	length, err := cboring.ReadArrayLength(r)
	if err != nil {
		return err
	}

	bundles := make([]bundle.BundleID, length)
	for ; length > 0; length-- {
		bundleID := bundle.BundleID{}
		if err := cboring.Unmarshal(&bundleID, r); err != nil {
			return err
		}
		bundles[length] = bundleID
	}

	*rapidBlock = bundles
	return nil
}
