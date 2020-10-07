// SPDX-FileCopyrightText: 2020 Alvar Penning
// SPDX-FileCopyrightText: 2020 Markus Sommer
//
// SPDX-License-Identifier: GPL-3.0-or-later

package cla

// CLAType is one of the supported Convergence Layer Adaptors
type CLAType uint

const (
	// TCPCLv4 identifies the Delay-Tolerant Networking TCP Convergence Layer Protocol Version 4, implemented in cla/tcpclv4.
	TCPCLv4 CLAType = 0

	// TCPCLv4WebSocket identifies a variation of TCPCLv4 based on WebSockets.
	TCPCLv4WebSocket CLAType = 1

	// MTCP identifies the Minimal TCP Convergence-Layer Protocol, implemented in cla/mtcp.
	MTCP CLAType = 10

	// BBC identifies the Bundle Broadcasting Connector, implemented in cla/bbc.
	// Only here for completeness
	BBC CLAType = 20
)
