package core

import (
	"github.com/dtn7/cboring"
	"github.com/dtn7/dtn7-go/bundle"
	"github.com/dtn7/dtn7-go/cla"
	log "github.com/sirupsen/logrus"
	"io"
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
	// TODO: Dummy Implementation
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

	for _, bundleID := range *rapidBlock {
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
