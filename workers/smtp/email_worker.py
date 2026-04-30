import asyncio
import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from aio_pika.abc import AbstractIncomingMessage

from core.config.config import settings
from core.config.config import EMAIL_QUEUE
from core.logger.logger import logger
from core.rabbitmq.rabbitmq import rabbitmq
from workers.common import managed_worker_resources


def _send_smtp(body: dict) -> None:
    msg = MIMEMultipart("alternative")
    msg["Subject"] = body.get("subject", "")
    msg["From"] = body.get("from") or settings.EMAIL_FROM
    msg["To"] = body["to"]

    if body.get("html"):
        msg.attach(MIMEText(body["html"], "html"))

    if body.get("message"):
        msg.attach(MIMEText(body["message"], "plain"))

    with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT) as server:
        server.starttls()
        server.login(settings.SMTP_USER, settings.SMTP_PASSWORD)
        server.sendmail(msg["From"], [body["to"]], msg.as_string())


async def process_email(message: AbstractIncomingMessage) -> None:
    async with message.process():
        try:
            body = json.loads(message.body.decode())
            await asyncio.to_thread(_send_smtp, body)
        except Exception as e:
            logger.exception(e)
            raise


async def start_email_worker() -> None:
    async with managed_worker_resources(use_rabbitmq=True):
        assert rabbitmq.channel is not None

        await rabbitmq.channel.set_qos(prefetch_count=1)

        queue = await rabbitmq.channel.declare_queue(EMAIL_QUEUE, durable=True)

        await queue.consume(process_email)

        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(start_email_worker())
