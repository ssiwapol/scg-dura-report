# -*- coding: utf-8 -*-
import os
import argparse
import base64
import logging

import yaml

from channel.main import main as channel
from executive.main import main as executive


parser = argparse.ArgumentParser()
parser.add_argument("-l", "--local", action="store_true", help="run locally")
args = parser.parse_args()
if args.local:
    with open("config.yaml") as f:
        config = yaml.load(f, Loader=yaml.Loader)
    for k, v in config.items():
        os.environ[k] = v


def dura_pubsub(event, context):
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    if event and pubsub_message=="channel":
        channel()
    elif event and pubsub_message=="executive":
        executive("executive/arial.ttf")
    else:
        logging.error("No function")
