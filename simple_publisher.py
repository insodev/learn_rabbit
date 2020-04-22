import asyncio
import logging
import os

import aio_pika
from aio_pika.exceptions import CONNECTION_EXCEPTIONS

import round_robust

_, prefix = os.path.split(__file__)

for logger_name in ['custom_robust', 'aio_pika.robust_connection', 'round_robust', __name__]:
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    # create file handler which logs even debug messages
    fh = logging.FileHandler(f'./logs/{prefix}.{logger_name}.log')
    fh.setLevel(logging.DEBUG)
    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)


async def main(loop):
    urls = [
        'amqp://test:test@localhost:32783/test',
        'amqp://test:test@localhost:32789/test',
    ]

    connection = await round_robust.custom_connect(urls=urls, loop=loop)

    async with connection:
        routing_key = "test_queue"

        channel = await connection.channel()
        for i in range(60):
            message = '{} Hello {}'.format(routing_key, i)
            print(message)
            sent = False
            while not sent:
                try:
                    await channel.default_exchange.publish(
                        aio_pika.Message(body=message.encode()), routing_key=routing_key
                    )
                except CONNECTION_EXCEPTIONS:
                    pass
                else:
                    sent = True
                await asyncio.sleep(1)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    loop.close()
