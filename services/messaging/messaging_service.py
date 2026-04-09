import json

import aio_pika
from aio_pika import Message


async def publish(
    queue_name: str, payload: dict, channel: aio_pika.abc.AbstractChannel
) -> None:
    body = json.dumps(payload).encode()
    message = Message(body=body, delivery_mode=2)

    await channel.default_exchange.publish(message, routing_key=queue_name)
