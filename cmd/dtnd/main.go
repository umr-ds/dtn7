// SPDX-FileCopyrightText: 2019, 2020 Alvar Penning
//
// SPDX-License-Identifier: GPL-3.0-or-later

package main

import (
	"os"
	"os/signal"
	"net/http"

	"runtime"

	_ "net/http/pprof"

	log "github.com/sirupsen/logrus"
)

// waitSigint blocks the current thread until a SIGINT appears.
func waitSigint() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)

	<-sig
}

func main() {
	runtime.SetBlockProfileRate(1)
	runtime.SetMutexProfileFraction(1)
	go func() {
		log.Println(http.ListenAndServe("localhost:9999", nil))
	}()

	if len(os.Args) != 2 {
		log.Fatalf("Usage: %s configuration.toml", os.Args[0])
	}

	core, discovery, err := parseCore(os.Args[1])
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("Failed to parse config")
	}

	waitSigint()
	log.Info("Shutting down..")

	core.Close()

	if discovery != nil {
		discovery.Close()
	}
}
