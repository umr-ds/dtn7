function vectorAngle(xa, ya, xb, yb) {
    loggingFunc("vectorAngle");
    loggingFunc("xa: " + xa + ", xb: " + xb + ", ya: " + ya + ", yb: " + yb)
    var dot = (xa * xb) + (ya * yb);
    var magA = Math.sqrt(Math.pow(xa, 2) + Math.pow(ya, 2));
    var magB = Math.sqrt(Math.pow(xb, 2) + Math.pow(yb, 2));
    return Math.acos(dot / (magA * magB))
}

function sensorToSensor(peerID) {
    var ownDistance = JSON.parse(context["backbone"])["distance"];
    loggingFunc("ownDistance: " + ownDistance)
    var peerDistance = JSON.parse(peerContext[peerID]["backbone"])["distance"];
    loggingFunc("peerDistance: " + peerDistance)
    if (peerDistance < ownDistance) {
        return true;
    }

    var ownConnectedness = JSON.parse(context["connectedness"])["value"];
    loggingFunc("ownConnectedness: " + ownConnectedness)
    var peerConnectedness = JSON.parse(peerContext[peerID]["connectedness"])["value"];
    loggingFunc("peerConnectedness: " + peerConnectedness)
    return peerConnectedness > ownConnectedness;
}

function guestToGuest(peerID) {
    var ownVector = JSON.parse(context["movement"]);
    var peerVector = JSON.parse(peerContext[peerID]["movement"]);

    var ownSpeed = Math.sqrt(Math.pow(ownVector.x, 2) + Math.pow(ownVector.y, 2));
    loggingFunc("Own speed: " + ownSpeed);
    var peerSpeed = Math.sqrt(Math.pow(peerVector.x, 2) + Math.pow(peerVector.y, 2));
    loggingFunc("Peer speed: " + peerSpeed);
    return peerSpeed > ownSpeed;
}

loggingFunc("Peer List: " + peers);
loggingFunc("Own context: " + JSON.stringify(context));
loggingFunc("Aggregated Peer Context: " + JSON.stringify(peerContext))

var senders = [];

len = peers.length;
for (var i = 0; i < len; i++) {
    var currentPeer = peers[i];
    loggingFunc("Peer: " + currentPeer);

    var currentPeerContext = peerContext[currentPeer];
    loggingFunc("Peer Context: " + JSON.stringify(currentPeerContext));
    if (currentPeerContext === undefined) {
        continue;
    }

    var ownRole = JSON.parse(context["role"]);
    var ownType = ownRole["node_type"];
    loggingFunc("Own Type: " + ownType);


    var peerRole = JSON.parse(currentPeerContext["role"]);
    var peerType = peerRole["node_type"];
    loggingFunc("Peer Type: " + peerType);

    var forward = false;
    switch (ownType) {
        case "sensor":
            switch (peerType) {
                case "sensor":
                    loggingFunc("sensorToSensor");
                    forward = sensorToSensor(currentPeer);
                    break;
                case "backbone":
                    // backbones are now sinks, so we always forward
                    loggingFunc("sensorToSBackbone");
                    forward = true;
                    break;
                case "visitor":
                    loggingFunc("sensorToGuest")
                    forward = true;
                    break;
                default:
                    loggingFunc("Unknown node type: " + peerRole);
            }
            break;
        case "backbone":
            // For this scenario, we decided that backbone nodes should be sinks and therefore, they never forward
            forward = false;
            break;
        case "visitor":
            switch (peerType) {
                case "sensor":
                    loggingFunc("guestToSensor")
                    forward = false;
                    break;
                case "backbone":
                    loggingFunc("guestToBackbone");
                    // backbones are now sinks, so we always forward
                    forward = true;
                    break;
                case "visitor":
                    loggingFunc("guestToGuest");
                    forward = guestToGuest(currentPeer);
                    break;
                default:
                    loggingFunc("Unknown node type: " + peerRole);
            }
            break;
        default:
            loggingFunc("Unknown node type: " + ownRole);
    }

    loggingFunc("Will forward to peer " + currentPeer + ": " + forward);

    if (forward) {
        senders.push(currentPeer);
    }
}

senders;
