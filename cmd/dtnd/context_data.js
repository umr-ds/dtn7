loggingFunc("This is a test");

function vectorAngle(vecA, vecB) {
    var dot = (vecA.x * vecB.x) + (vecA.y * vecB.y);
    var magA = Math.sqrt(Math.pow(vecA.x, 2) + Math.pow(vecA.y, 2));
    var magB = Math.sqrt(Math.pow(vecB.x, 2) + Math.pow(vecB.y, 2));
    return Math.acos(dot / (magA * magB))
}

function sensorToSensor(peerID) {
    // check if peer accepts this type of data
    var bundleType = bundleContext["type"];
    var acceptedTypes = JSON.parse(peerContext[peerID]["accepted_types"]);
    if (!(acceptedTypes.includes(bundleType))) {
        return false;
    }

    // check peer's battery
    var peerBattery = JSON.parse(peerContext[peerID]["battery"]);
    var batteryLevel = peerBattery.level_abs;
    var lowThreshold = peerBattery.low_threshold;
    var peerBatteryLow = batteryLevel < lowThreshold;
    if (peerBatteryLow) {
        return false;
    }

    // check own battery
    var ownBattery = JSON.parse(context["battery"]);
    batteryLevel = ownBattery.level_abs;
    lowThreshold = ownBattery.low_threshold;
    var ownBatteryLow = batteryLevel < lowThreshold;
    if (ownBatteryLow && !peerBatteryLow) {
        return true;
    }

    var ownDistance = JSON.parse(context["backbone"]).distance;
    var peerDistance = JSON.parse(peerContext[peerID]["backbone"]).distance;
    if (peerDistance < ownDistance) {
        return true;
    }

    var ownConnectedness = JSON.parse(context["connectedness"]);
    var peerConnectedness = JSON.parse(peerContext[peerID]["connectedness"]);
    return peerConnectedness > ownConnectedness;
}

function sensorToBackbone(peerID) {
    var peerCongested = JSON.parse(peerContext[peerID]["congested"]);
    return !peerCongested;
}

function sensorToGuest(peerID) {
    var peerSubscriptions = JSON.parse(peerContext[peerID]["subscriptions"]);
    var bundleChannel = JSON.parse(bundleConext["channel"]);
    if (peerSubscriptions.includes(bundleChannel)) {
        return true;
    }

    var peerMule = JSON.parse(peerContext[peerID]["mule"]);
    if (!peerMule) {
        return false
    }

    var bundleType = bundleContext["type"];
    var acceptedTypes = JSON.parse(peerContext[peerID]["accepted_types"]);
    if (!acceptedTypes.contains(bundleType)) {
        return false;
    }

    var bundleDestination = JSON.parse(context["destination"]);
    var nodeVector = JSON.parse(peerContext[peerID]["movement"]);
    var angle = vectorAngle(bundleDestination, nodeVector);
    return angle < 0.79
}

function backboneToSensor(peerID) {
    return false;
}

function backboneToBackbone(peerID) {
    var peerCongested = JSON.parse(peerContext[peerID]["congested"]);
    if (peerCongested) {
        return false;
    }

    var congested = JSON.parse(context["congested"]);
    if (congested) {
        return true;
    }

    var speed = JSON.parse(context["uplink_speed"]);
    var peerSpeed = JSON.parse(peerContext[peerID]["uplink_speed"]);
    return peerSpeed > speed;
}

function backboneToGuest(peerID) {
    var peerSubscriptions = JSON.parse(peerContext[peerID]["subscriptions"]);
    var bundleChannel = JSON.parse(bundleConext["channel"]);
    return peerSubscriptions.includes(bundleChannel);
}

function guestToSensor(peerID) {
    return false;
}

function guestToBackbone(peerID) {
    var peerCongested = JSON.parse(peerContext[peerID]["congested"]);
    return !peerCongested;
}

function guestToGuest(peerID) {
    var peerSubscriptions = JSON.parse(peerContext[peerID]["subscriptions"]);
    var bundleChannel = JSON.parse(bundleConext["channel"]);
    if (peerSubscriptions.includes(bundleChannel)) {
        return true;
    }

    var peerMule = JSON.parse(peerContext[peerID]["mule"]);
    if (!peerMule) {
        return false
    }

    var bundleType = bundleContext["type"];
    var acceptedTypes = JSON.parse(peerContext[peerID]["accepted_types"]);
    if (!acceptedTypes.contains(bundleType)) {
        return false;
    }

    var ownVector = JSON.parse(context["movement"]);
    var peerVector = JSON.parse(peerContext[peerID]["movement"]);
    var angle = vectorAngle(ownVector, peerVector);
    if (angle > 0.79) {
        return false;
    }

    var ownSpeed = Math.sqrt(Math.pow(ownVector.x, 2) + Math.pow(ownVector.y, 2));
    var peerSpeed = Math.sqrt(Math.pow(peerVector.x, 2) + Math.pow(peerVector.y, 2));
    return peerSpeed > ownSpeed;
}

loggingFunc("Peer List: " + peers);

var senders = [];

len = peers.length;
for (var i = 0; i < len; i++) {
    var peer = peers[i];
    loggingFunc("Peer: " + peer);

    var ownRole = JSON.parse(context["role"]);
    var peerRole = JSON.parse(peerContext[peer]["role"]);
    var forward = false;
    switch (ownRole) {
        case "sensor":
            switch (peerRole) {
                case "sensor":
                    forward = sensorToSensor(peer);
                    break;
                case "backbone":
                    forward = sensorToBackbone(peer);
                    break;
                case "guest":
                    forward = sensorToGuest(peer);
                    break;
                default:
                    loggingFunc("Unknown node type: " + peerRole);
            }
            break;
        case "backbone":
            switch (peerRole) {
                case "sensor":
                    forward = backboneToSensor(peer);
                    break;
                case "backbone":
                    forward = backboneToBackbone(peer);
                    break;
                case "guest":
                    forward = backboneToGuest(peer);
                    break;
                default:
                    loggingFunc("Unknown node type: " + peerRole);
            }
            break;
        case "guest":
            switch (peerRole) {
                case "sensor":
                    forward = guestToSensor(peer);
                    break;
                case "backbone":
                    forward = guestToBackbone(peer);
                    break;
                case "guest":
                    forward = guestToGuest(peer);
                    break;
                default:
                    loggingFunc("Unknown node type: " + peerRole);
            }
            break;
        default:
            loggingFunc("Unknown node type: " + ownRole);
    }

     if (forward) {
         senders.push(peer);
     }
}

senders;
