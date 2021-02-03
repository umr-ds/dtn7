var senders = [];

copies = JSON.parse(bundleContext["copies"]);
loggingFunc("Copies of bundle " + bundleID +" left: " + copies)

if (copies > 0 && peers.length > 0) {
    copies = Math.floor(copies / 2);
    senders = [peers[0]];
    bundleContext["copies"] = copies.toString();
    modifyBundleContext(bndl, bundleContext);
}

copies = JSON.parse(bundleContext["copies"]);
loggingFunc("Copies of bundle " + bundleID + " left after eval: " + copies)

senders;