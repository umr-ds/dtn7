function vectorAngle(xa, ya, xb, yb) {
    loggingFunc("vectorAngle");
    var dot = (xa * xb) + (ya * yb);
    var magA = Math.sqrt(Math.pow(xa, 2) + Math.pow(ya, 2));
    var magB = Math.sqrt(Math.pow(xb, 2) + Math.pow(yb, 2));
    return Math.acos(dot / (magA * magB))
}

function sensorToSensor(peerID) {
    loggingFunc("sensorToSensor");

    var distanceJSON = context["backbone"];
    loggingFunc("Own distance: " + distanceJSON);
    var ownDistance = JSON.parse(distanceJSON).distance;

    var peerJSON = peerContext[peerID]["backbone"];
    loggingFunc("Peer distance: " + peerJSON);
    var peerDistance = JSON.parse(peerJSON).distance;
    if (peerDistance < ownDistance) {
        return true;
    }

    var conJSON = context["connectedness"];
    loggingFunc("Own connectedness: " + conJSON);
    var ownConnectedness = JSON.parse(conJSON)["value"];

    peerJSON = peerContext[peerID]["connectedness"];
    loggingFunc("Peer connectedness: " + peerJSON);
    var peerConnectedness = JSON.parse(peerJSON)["value"];
    return peerConnectedness > ownConnectedness;
}

function sensorToBackbone() {
    loggingFunc("sensorToBackbone");
    return true;
}

function sensorToGuest(peerID) {
    loggingFunc("sensorToGuest");
    loggingFunc("Bundle context: " + bundleContext);
    var bndlContext = JSON.parse(bundleContext);

    var vectorJSON = peerContext[peerID]["movement"];
    loggingFunc("vectorJSON: " + vectorJSON);
    var nodeVector = JSON.parse(vectorJSON);

    var angle = vectorAngle(bndlContext.x_dest, bndlContext.y_dest, nodeVector.x, nodeVector.y);
    loggingFunc("Angle: " + angle);
    return angle < 0.79
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

    var vectorJSON = context["movement"];
    loggingFunc("Own movement: " + vectorJSON);
    var ownVector = JSON.parse(vectorJSON);

    var peerJSON = peerContext[peerID]["movement"];
    loggingFunc("Peer movement: " + peerJSON);
    var peerVector = JSON.parse(peerJSON);
    var angle = vectorAngle(ownVector.x, ownVector.y, peerVector.x, peerVector.y);
    loggingFunc("Angle: " + angle);
    if (angle > 0.79) {
        return false;
    }

    var ownSpeed = Math.sqrt(Math.pow(ownVector.x, 2) + Math.pow(ownVector.y, 2));
    loggingFunc("Own speed: " + ownSpeed);
    var peerSpeed = Math.sqrt(Math.pow(peerVector.x, 2) + Math.pow(peerVector.y, 2));
    loggingFunc("Peer speed: " + peerSpeed);
    return peerSpeed > ownSpeed;
}

loggingFunc("Peer List: " + peers);

var senders = [];

len = peers.length;
for (var i = 0; i < len; i++) {
    var peer = peers[i];
    loggingFunc("Peer: " + peer);

    var roleJSON = context["role"];
    //loggingFunc("roleJSON: " + roleJSON);
    var ownRole = JSON.parse(roleJSON)["node_type"];
    loggingFunc("Own Role: " + ownRole);

    var thisPeer = peerContext[peer];
    if (thisPeer === undefined) {
        continue;
    }

    var peerRole = JSON.parse(thisPeer["role"])["node_type"];
    loggingFunc("Peer Role: " + peerRole);

    var forward = false;
    switch (ownRole) {
        case "sensor":
            switch (peerRole) {
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
            switch (peerRole) {
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
