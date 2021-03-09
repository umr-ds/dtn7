// SPDX-FileCopyrightText: 2021 Markus Sommer
//
// SPDX-License-Identifier: GPL-3.0-or-later

package bpv7

import (
	"fmt"
	"io"

	"github.com/dtn7/cboring"
)

const (
	ChronicleCreateStream string = "create"
	ChronicleInsertEvents string = "insert"
)

const chronicleBlockFields uint64 = 2

type ChronicleBlock struct {
	Operation  string
	StreamName string
}

func (cb *ChronicleBlock) CheckValid() error {
	if (cb.Operation != ChronicleCreateStream) && (cb.Operation != ChronicleInsertEvents) {
		return fmt.Errorf("invalid operation")
	}

	if cb.StreamName == "" {
		return fmt.Errorf("stream name must not be empty")
	}

	return nil
}

func (cb *ChronicleBlock) BlockTypeCode() uint64 {
	return ExtBlockTypeChronicleBlock
}

func (cb *ChronicleBlock) BlockTypeName() string {
	return "ChronicleDB Block"
}

func (cb *ChronicleBlock) MarshalCbor(w io.Writer) error {
	if err := cboring.WriteArrayLength(chronicleBlockFields, w); err != nil {
		return err
	}

	if err := cboring.WriteTextString(cb.Operation, w); err != nil {
		return err
	}

	if err := cboring.WriteTextString(cb.StreamName, w); err != nil {
		return err
	}

	return nil
}

func (cb *ChronicleBlock) UnmarshalCbor(r io.Reader) error {
	if l, err := cboring.ReadArrayLength(r); err != nil {
		return err
	} else if l != chronicleBlockFields {
		return fmt.Errorf("expected %d fields, got %d", chronicleBlockFields, l)
	}

	if operation, err := cboring.ReadTextString(r); err != nil {
		return err
	} else {
		cb.Operation = operation
	}

	if streamName, err := cboring.ReadTextString(r); err != nil {
		return err
	} else {
		cb.StreamName = streamName
	}

	return nil
}

// ChronicleBlock builds a ChronicleBlock from an operation and a name
func (bldr *BundleBuilder) ChronicleBlock(args []interface{}) *BundleBuilder {
	if bldr.err != nil {
		return bldr
	}

	operation, ok := args[0].(string)
	if !ok {
		bldr.err = fmt.Errorf("did not provide chronicle operation")
		return bldr
	}

	streamName, ok := args[1].(string)
	if !ok {
		bldr.err = fmt.Errorf("did not provide stream name")
	}

	chronicleBlock := &ChronicleBlock{
		Operation:  operation,
		StreamName: streamName,
	}

	if err := chronicleBlock.CheckValid(); err != nil {
		bldr.err = err
		return bldr
	}

	return bldr.Canonical(chronicleBlock, DeleteBundle)
}
