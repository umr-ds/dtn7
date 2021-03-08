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
	ChronicleCreateStream uint64 = iota
	ChronicleInsertEvents
)

const chronicleBlockFields uint64 = 2

type ChronicleBlock struct {
	Operation  uint64
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

	if err := cboring.WriteUInt(cb.Operation, w); err != nil {
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

	if operation, err := cboring.ReadUInt(r); err != nil {
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
