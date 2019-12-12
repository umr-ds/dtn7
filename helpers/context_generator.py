#! /usr/bin/env python3

import argparse
import json
import requests
from typing import Any, Dict
from time import sleep
from random import randint, choice

REST_ADDRESS = "127.0.0.1"
CONTEXT_PORT = 35043


def build_url(address: str, port: int) -> str:
    return f"http://{address}:{port}"


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


def send_integer(url: str, name: str) -> None:
    print(f"Sending random integer context with name '{name}'")
    value = {"value": randint(1, 10)}
    send_context(rest_url=url, context_name=name, node_context=value)


def send_random_context(url: str, context: Dict[str, Any]) -> None:
    """Pick and send a random context-item to the node"""
    name: str = choice(context.keys())
    send_context(rest_url=url, context_name=name, node_context=context[name])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Interact with dtnd")
    # parser.add_argument("context_file")
    args = parser.parse_args()

    print("Starting context generator")
    # context_collection: Dict[str, Any] = load_context(path=args.context_file)
    context_rest: str = build_url(address=REST_ADDRESS, port=CONTEXT_PORT)

    sleep(10)

    while True:
        # send_random_context(url=context_rest, context=context_collection)
        send_integer(url=context_rest, name="fitness")
        sleep(randint(1, 20))
