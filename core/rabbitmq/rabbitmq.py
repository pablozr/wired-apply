from aio_pika import connect_robust
from aio_pika.abc import AbstractChannel

from core.config.config import settings


class RabbitMQ:
    connection = None
    channel: AbstractChannel | None = None

    async def connect(self):
        self.connection = await connect_robust(
            f"amqp://{settings.RABBITMQ_USER}:{settings.RABBITMQ_PASSWORD}@{settings.RABBITMQ_HOST}:{settings.RABBITMQ_PORT}/"
        )
        self.channel = await self.connection.channel()

    async def disconnect(self):
        await self.connection.close()

    async def get_channel(self):
        yield self.channel


rabbitmq = RabbitMQ()
