package core

import (
	"fmt"
	"strings"
	"time"

	"github.com/dtn7/dtn7-go/bundle"
)

// BundlePack is a set of a bundle, it's creation or reception time stamp and
// a set of constraints used in the process of delivering this bundle.
type BundlePack struct {
	Bundle      *bundle.Bundle
	Receiver    bundle.EndpointID
	Timestamp   time.Time
	Constraints map[Constraint]bool
}

// NewBundlePack returns a BundlePack for the given bundle.
func NewBundlePack(b *bundle.Bundle) BundlePack {
	return BundlePack{
		Bundle:      b,
		Receiver:    bundle.DtnNone(),
		Timestamp:   time.Now(),
		Constraints: make(map[Constraint]bool),
	}
}

// ID returns the wrapped Bundle's ID.
func (bp BundlePack) ID() string {
	return bp.Bundle.ID().String()
}

// HasReceiver returns true if this BundlePack has a Receiver value.
func (bp BundlePack) HasReceiver() bool {
	return bp.Receiver != bundle.DtnNone()
}

// HasConstraint returns true if the given constraint contains.
func (bp BundlePack) HasConstraint(c Constraint) bool {
	_, ok := bp.Constraints[c]
	return ok
}

// HasConstraints returns true if any constraint exists.
func (bp BundlePack) HasConstraints() bool {
	return len(bp.Constraints) != 0
}

// AddConstraint adds the given constraint.
func (bp *BundlePack) AddConstraint(c Constraint) {
	bp.Constraints[c] = true
}

// RemoveConstraint removes the given constraint.
func (bp *BundlePack) RemoveConstraint(c Constraint) {
	delete(bp.Constraints, c)
}

// PurgeConstraints removes all constraints, except LocalEndpoint.
func (bp *BundlePack) PurgeConstraints() {
	for c := range bp.Constraints {
		if c != LocalEndpoint {
			bp.RemoveConstraint(c)
		}
	}
}

// UpdateBundleAge updates the bundle's Bundle Age block based on its reception
// timestamp, if such a block exists.
func (bp *BundlePack) UpdateBundleAge() (uint64, error) {
	ageBlock, err := bp.Bundle.ExtensionBlock(bundle.ExtBlockTypeBundleAgeBlock)
	if err != nil {
		return 0, newCoreError("No such block")
	}

	age := ageBlock.Value.(*bundle.BundleAgeBlock)
	return age.Increment(uint64(time.Now().Sub(bp.Timestamp)) / 1000), nil
}

func (bp BundlePack) String() string {
	var b strings.Builder

	fmt.Fprintf(&b, "BundlePack(%v,", bp.ID())

	for c := range bp.Constraints {
		fmt.Fprintf(&b, " %v", c)
	}

	if bp.HasReceiver() {
		fmt.Fprintf(&b, ", %v", bp.Receiver)
	}

	fmt.Fprintf(&b, ")")

	return b.String()
}
