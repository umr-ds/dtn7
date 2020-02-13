#! /usr/bin/env python3

import argparse
import base64
import json
from typing import Any, Dict, List

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


def load_context(path: str) -> Dict[str, Any]:
    """Load bundle context from provided file and parse it to check if it is valid JSON

    Args:
        path (str): Path of the context-file

    Return:
        str:
    """
    with open(path, "r") as f:
        contents: str = f.read()
        # try to parse json to see if it is valid
        decoded = json.loads(contents)
        return decoded


def build_url(address: str, port: int) -> str:
    return f"http://{address}:{port}"


def send_bundle(
    rest_url: str,
    bundle_recipient: str,
    bundle_context: Dict[str, Any],
    bundle_payload: str,
) -> None:
    """Send a bundle via the context-REST agent

    Args:
        rest_url (str): URL of the REST-interface
        bundle_recipient (str): DTN EndpointID of the bundle's recipient
        bundle_context (Dict[str, Any]): Bundle's context
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
    if response.status_code != 202:
        print(f"Status: {response.status_code}")
    print(response.text)


def send_context(
    rest_url: str, context_name: str, node_context: Dict[str, Any]
) -> None:
    """Sends node context information to the routing daemon

    Args:
        rest_url (str): URL of the REST-interface
        context_name (str): name of the context item
        node_context (Dict[str, Any]): Actual context
    """
    contest_str: str = json.dumps(node_context)
    response: requests.Response = requests.post(
        f"{rest_url}/context/{context_name}", data=contest_str
    )
    if response.status_code != 202:
        print(f"Status: {response.status_code}")
    print(response.text)


def get_node_context(rest_url: str) -> None:
    """Get all the Node's context information

    Args:
        rest_url (str): URL of the REST-interface
    """
    response: requests.Response = requests.get(f"{rest_url}/context")
    if response.status_code != 200:
        print(f"Status: {response.status_code}")
    print(response.text)


def get_pending(rest_url: str) -> None:
    """Get contents of stored bundle buffer

    Args:
        rest_url (str): URL of the REST-interface
    """
    response: requests.Response = requests.get(f"{rest_url}/pending")
    if response.status_code != 200:
        print(f"Status: {response.status_code}")
        print(response.text)
    else:
        bundles: List[str] = json.loads(response.text)
        for bundle in bundles:
            print(bundle)


def get_size(rest_url: str) -> None:
    """Get size of stored bundle buffer

    Args:
        rest_url (str): URL of the REST-interface
    """

    response: requests.Response = requests.get(f"{rest_url}/size")
    if response.status_code != 200:
        print(f"Status: {response.status_code}")
    print(response.text)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Interact with dtnd")
    parser.add_argument("interface", help="One of [bundle, context]")
    parser.add_argument("action", help="One of [send, get, size]")
    parser.add_argument("-p", "--payload", help="Specify payload file")
    parser.add_argument("-c", "--context", help="Specify context file")
    parser.add_argument(
        "-cn", "--context_name", help="When sending node context, supply its name"
    )
    parser.add_argument(
        "-a", "--address", default="localhost", help="Address of the REST-interface"
    )
    parser.add_argument(
        "-pb",
        "--port_bundle",
        type=int,
        default=35038,
        help="Port of the bundle REST-interface",
    )
    parser.add_argument(
        "-pc",
        "--port_context",
        type=int,
        default=35043,
        help="Port of the context REST-interface",
    )
    parser.add_argument(
        "-r", "--recipient", help="DTN-EndpointID of the bundle's recipient"
    )
    args = parser.parse_args()

    if args.interface == "bundle":
        url = build_url(address=args.address, port=args.port_bundle)

        if args.action == "send":
            payload = load_payload(path=args.payload)
            context = load_context(path=args.context)

            send_bundle(
                rest_url=url,
                bundle_recipient=args.recipient,
                bundle_context=context,
                bundle_payload=payload,
            )
        elif args.action == "get":
            get_pending(rest_url=url)
        elif args.action == "size":
            get_size(rest_url=url)
        else:
            print("UNSUPPORTED ACTION")
    elif args.interface == "context":
        url = build_url(address=args.address, port=args.port_context)

        if args.action == "send":
            context = load_context(path=args.context)
            send_context(
                rest_url=url, context_name=args.context_name, node_context=context
            )
        elif args.action == "get":
            get_node_context(rest_url=url)
        else:
            print("UNSUPPORTED ACTION")
    else:
        print("UNKNOWN INTERFACE")
