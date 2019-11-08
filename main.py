import base64

import yaml

from channel import main as channel
from executive import main as executive


def dura_pubsub(event, context):
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    if event and pubsub_message=="channel":
        with open("channel/config.yaml") as f:
            config = yaml.load(f, Loader=yaml.Loader)
        channel.main(config)
    elif event and pubsub_message=="executive":
        with open("executive/config.yaml") as f:
            config = yaml.load(f, Loader=yaml.Loader)
        executive.main(config, "executive/arial.ttf")
