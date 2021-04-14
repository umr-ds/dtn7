function vectorAngle(xa, ya, xb, yb) {
    loggingFunc("vectorAngle");
    loggingFunc("xa: " + xa + ", xb: " + xb + ", ya: " + ya + ", yb: " + yb)
    var dot = (xa * xb) + (ya * yb);
    var magA = Math.sqrt(Math.pow(xa, 2) + Math.pow(ya, 2));
    var magB = Math.sqrt(Math.pow(xb, 2) + Math.pow(yb, 2));
    return Math.acos(dot / (magA * magB))
}

function sensorToSensor(peerID) {
    loggingFunc("sensorToSensor");

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

function sensorToBackbone() {
    loggingFunc("sensorToBackbone");
    return true;
}

function sensorToGuest(peerID) {
    loggingFunc("sensorToGuest");
    return true;
}

function backboneToSensor() {
    loggingFunc("backboneToSensor");
    return false;
}

function backboneToBackbone() {
    loggingFunc("backboneToBackbone");
    return false;
}

function backboneToGuest() {
    loggingFunc("backboneToGuest");
    return false;
}

function guestToSensor() {
    loggingFunc("guestToSensor");
    return false;
}

function guestToBackbone() {
    loggingFunc("guestToBackbone");
    return true;
}

function guestToGuest(peerID) {
    loggingFunc("guestToGuest");

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
    var peer = peers[i];
    loggingFunc("Peer: " + peer);

    var thisPeer = peerContext[peer];
    loggingFunc("Peer Context: " + JSON.stringify(thisPeer));
    if (thisPeer === undefined) {
        continue;
    }

    var ownRole = JSON.parse(context["role"]);
    var ownType = ownRole["node_type"];
    loggingFunc("Own Type: " + ownType);


    var peerRole = JSON.parse(thisPeer["role"]);
    var peerType = peerRole["node_type"];
    loggingFunc("Peer Type: " + peerType);

    var forward = false;
    switch (ownType) {
        case "sensor":
            switch (peerType) {
                case "sensor":
                    forward = sensorToSensor(peer);
                    break;
                case "backbone":
                    // backbones are now sinks, so we always forward
                    forward = true;
                    break;
                case "visitor":
                    forward = sensorToGuest(peer);
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
                    forward = guestToSensor(peer);
                    break;
                case "backbone":
                    // backbones are now sinks, so we always forward
                    forward = true;
                    break;
                case "visitor":
                    forward = guestToGuest(peer);
                    break;
                default:
                    loggingFunc("Unknown node type: " + peerRole);
            }
            break;
        default:
            loggingFunc("Unknown node type: " + ownRole);
    }

    loggingFunc("Will forward to peer " + peer + ": " + forward);

    if (forward) {
        senders.push(peer);
    }
}

senders;
