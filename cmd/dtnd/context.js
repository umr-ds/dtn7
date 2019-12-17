loggingFunc("JAVASCRIPT: This is a test");

var senders = [];

loggingFunc(peers);

len = peers.length;
for (i = 0; i < len; i++) {
    peer = peers[i];
    var fitness = JSON.parse(peerContext[peer]["fitness"]).value;
    loggingFunc(fitness.toString());
    var bundleFitness = JSON.parse(bundleContext["fitness"]);
    loggingFunc(bundleFitness.toString());

    if (fitness > bundleFitness) {
        senders.push(peer)
    }
}

senders;
