loggingFunc("JAVASCRIPT: This is a test");

var senders = ["",];

var peer;
for (peer in peers) {
    loggingFunc(peer);
    var fitness = JSON.parse(peerContext[peer]["fitness"]).value;
    loggingFunc(fitness.toString());
    var bundleFitness = JSON.parse(bundleContext["fitness"]).value;
    loggingFunc(bundleFitness.toString());

    if (fitness > bundleFitness) {
        senders.push(peer)
    }
}

senders;
