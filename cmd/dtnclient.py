#! /usr/bin/env python3

import argparse
import base64
import json
import requests


def load_payload(path: str) -> str:
    """Loads payload from specified file

    Args:
        path (str): Path to the file containing payload

    Returns:
        str: Content of payload file (encoded as base64 ist content was binary)
    """
    with open(path, "rb") as f:
        contents: bytes = f.read()
        return str(base64.b64encode(contents), encoding="utf-8")


def load_context(path: str) -> str:
    """Load bundle context from provided file and parse it to check if it is valid JSON

    Args:
        path (str): Path of the context-file

    Return:
        str:
    """
    with open(path, "r") as f:
        contents: str = f.read()
        # try to parse json to see if it is valid
        _ = json.loads(contents)
        return contents


def build_url(address: str, port: int) -> str:
    return f"http://{address}:{port}"


def send_bundle(
    rest_url: str, bundle_recipient: str, bundle_context: str, bundle_payload: str
) -> None:
    """Send a bundle via the context-REST agent

    Args:
        rest_url (str): URL of the REST-interface
        bundle_recipient (str): DTN EndpointID of the bundle's recipient
        bundle_context (str): Bundle's context
        bundle_payload (str): Bundle payload
    """

    bundle_data: str = json.dumps(
        {
            "recipient": bundle_recipient,
            "context": bundle_context,
            "payload": bundle_payload,
        }
    )

    response: requests.Response = requests.post(f"{rest_url}/send", data=bundle_data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Send and receive bundles")
    parser.add_argument("-p", "--payload", help="Specify payload file")
    parser.add_argument("-c", "--context", help="Specify context file")
    parser.add_argument(
        "-a", "--address", default="localhost", help="Address of the REST-interface"
    )
    parser.add_argument(
        "-po", "--port", type=int, default=35038, help="Port of the REST-interface"
    )
    parser.add_argument(
        "-r", "--recipient", help="DTN-EndpointID of the bundle's recipient"
    )
    args = parser.parse_args()

    payload = load_payload(path=args.payload)
    context = load_context(path=args.context)
    url = build_url(address=args.address, port=args.port)

    send_bundle(
        rest_url=url,
        bundle_recipient=args.recipient,
        bundle_context=context,
        bundle_payload=payload,
    )
