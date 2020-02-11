package main

import (
	"io"
	"io/ioutil"
	"os"

	"github.com/dtn7/dtn7-go/bundle"
)

// createBundle for the "create" CLI option.
func createBundle(args []string) {
	if len(args) != 4 {
		printUsage()
	}

	var (
		sender    = args[0]
		receiver  = args[1]
		dataInput = args[2]
		outName   = args[3]

		err  error
		data []byte
		b    bundle.Bundle
		f    io.WriteCloser
	)

	if dataInput == "-" {
		data, err = ioutil.ReadAll(os.Stdin)
	} else {
		data, err = ioutil.ReadFile(dataInput)
	}
	if err != nil {
		printFatal(err, "Reading input errored")
	}

	b, err = bundle.Builder().
		CRC(bundle.CRC32).
		Source(sender).
		Destination(receiver).
		CreationTimestampNow().
		Lifetime("24h").
		HopCountBlock(64).
		PayloadBlock(data).
		Build()
	if err != nil {
		printFatal(err, "Building Bundle errored")
	}

	if outName == "-" {
		f = os.Stdout
	} else if f, err = os.Create(outName); err != nil {
		printFatal(err, "Creating file errored")
	}

	if err = b.MarshalCbor(f); err != nil {
		printFatal(err, "Writing Bundle errored")
	}
	if err = f.Close(); err != nil {
		printFatal(err, "Closing file errored")
	}
}