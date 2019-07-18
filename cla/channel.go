package cla

import (
	"sync"
)

// merge merges two ConvergenceStatus channels into a new one.
func merge(a, b chan ConvergenceStatus) (ch chan ConvergenceStatus) {
	var wg sync.WaitGroup
	wg.Add(2)

	ch = make(chan ConvergenceStatus)

	for _, c := range []chan ConvergenceStatus{a, b} {
		go func(c chan ConvergenceStatus) {
			for bndl := range c {
				ch <- bndl
			}

			wg.Done()
		}(c)
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	return
}

// zeroChan is an empty, always opened channel - after being requested the first
// time from getZeroChan() - to be returned for an empty parameter list of
// JoinReceivers. This prevents the select statement within
// checkConvergenceReceivers's for loop to always return a closed channel and
// heat up the loop.
var zeroChan chan ConvergenceStatus

func getZeroChan() chan ConvergenceStatus {
	if zeroChan == nil {
		zeroChan = make(chan ConvergenceStatus)
	}

	return zeroChan
}

// JoinReceivers joins the given receiving bundle channels into a new channel,
// containing all bundles from all channels.
func JoinReceivers(chans ...chan ConvergenceStatus) chan ConvergenceStatus {
	switch len(chans) {
	case 0:
		return getZeroChan()

	case 1:
		return chans[0]

	default:
		pivot := len(chans) / 2

		left := JoinReceivers(chans[pivot:]...)
		right := JoinReceivers(chans[:pivot]...)

		return merge(left, right)
	}
}
