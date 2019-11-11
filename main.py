import base64
import logging

import yaml

from channel.main import main as channel
from executive.main import main as executive


def main(event, context):
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    if event and pubsub_message=="channel":
        with open("channel/config.yaml") as f:
            channel_config = yaml.load(f, Loader=yaml.Loader)
        channel(channel_config)
    elif event and pubsub_message=="executive":
        with open("executive/config.yaml") as f:
            executive_config = yaml.load(f, Loader=yaml.Loader)
        executive(executive_config, "executive/arial.ttf")
    else:
        logging.error("No function")
