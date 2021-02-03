function vectorAngle(xa, ya, xb, yb) {
    loggingFunc("vectorAngle");
    var dot = (xa * xb) + (ya * yb);
    var magA = Math.sqrt(Math.pow(xa, 2) + Math.pow(ya, 2));
    var magB = Math.sqrt(Math.pow(xb, 2) + Math.pow(yb, 2));
    return Math.acos(dot / (magA * magB))
}

function sensorToSensor(peerID) {
    loggingFunc("sensorToSensor");
    /*
    // check if peer accepts this type of data
    var bundleType = bundleContext["type"];
    var acceptedTypes = JSON.parse(peerContext[peerID]["accepted_types"]);
    if (!(acceptedTypes.includes(bundleType))) {
        return false;
    }
     */

    /*
    // check peer's battery
    var peerBattery = JSON.parse(peerContext[peerID]["battery"]);
    var batteryLevel = peerBattery.level_abs;
    var lowThreshold = peerBattery.low_threshold;
    var peerBatteryLow = batteryLevel < lowThreshold;
    if (peerBatteryLow) {
        return false;
    }
     */

    /*
    // check own battery
    var ownBattery = JSON.parse(context["battery"]);
    batteryLevel = ownBattery.level_abs;
    lowThreshold = ownBattery.low_threshold;
    var ownBatteryLow = batteryLevel < lowThreshold;
    if (ownBatteryLow && !peerBatteryLow) {
        return true;
    }
     */

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

function sensorToBackbone(peerID) {
    var peerCongested = JSON.parse(peerContext[peerID]["congested"]);
    return !peerCongested;
}

function sensorToGuest(peerID) {
    loggingFunc("sensorToGuest");
    /*
    var peerSubscriptions = JSON.parse(peerContext[peerID]["subscriptions"]);
    var bundleChannel = JSON.parse(bundleConext["channel"]);
    if (peerSubscriptions.includes(bundleChannel)) {
        return true;
    }
     */

    /*
    var peerMule = JSON.parse(peerContext[peerID]["mule"]);
    if (!peerMule) {
        return false
    }
     */

    /*
    var bundleType = bundleContext["type"];
    var acceptedTypes = JSON.parse(peerContext[peerID]["accepted_types"]);
    if (!acceptedTypes.contains(bundleType)) {
        return false;
    }
     */
    loggingFunc("Bundle context: " + bundleContext);
    var bndlContext = JSON.parse(bundleContext);

    var vectorJSON = peerContext[peerID]["movement"];
    loggingFunc("vectorJSON: " + vectorJSON);
    var nodeVector = JSON.parse(vectorJSON);

    var angle = vectorAngle(bndlContext.x_dest, bndlContext.y_dest, nodeVector.x, nodeVector.y);
    loggingFunc("Angle: " + angle);
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
    loggingFunc("guestToSensor");
    return false;
}

function guestToBackbone(peerID) {
    var peerCongested = JSON.parse(peerContext[peerID]["congested"]);
    return !peerCongested;
}

function guestToGuest(peerID) {
    loggingFunc("guestToGuest");
    /*
    var peerSubscriptions = JSON.parse(peerContext[peerID]["subscriptions"]);
    var bundleChannel = JSON.parse(bundleConext["channel"]);
    if (peerSubscriptions.includes(bundleChannel)) {
        return true;
    }
     */

    /*
    var peerMule = JSON.parse(peerContext[peerID]["mule"]);
    if (!peerMule) {
        return false
    }
     */

    /*
    var bundleType = bundleContext["type"];
    var acceptedTypes = JSON.parse(peerContext[peerID]["accepted_types"]);
    if (!acceptedTypes.contains(bundleType)) {
        return false;
    }
     */

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
