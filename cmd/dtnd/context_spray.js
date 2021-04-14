var senders = [];

copies = bundleContext["copies"];
loggingFunc("Copies of bundle " + bundleID +" left: " + copies)

if (copies > 0 && peers.length > 0) {
    copies = Math.floor(copies / 2);
    senders = [peers[0]];
    bundleContext["copies"] = copies;
    modifyBundleContext(bndl, bundleContext);
}

copies = bundleContext["copies"];
loggingFunc("Copies of bundle " + bundleID + " left after eval: " + copies)

senders;