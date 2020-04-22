import asyncio
import logging.config
import os

import round_robust

# create logger with 'spam_application'

_, prefix = os.path.split(__file__)

for logger_name in [__name__, 'custom_robust', 'aio_pika.robust_connection', 'round_robust']:
    logger_rmq = logging.getLogger(logger_name)
    logger_rmq.setLevel(logging.DEBUG)
    # create file handler which logs even debug messages
    fh = logging.FileHandler(f'./logs/{prefix}{logger_name}.log')
    fh.setLevel(logging.DEBUG)
    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger_rmq.addHandler(fh)
    logger_rmq.addHandler(ch)

URLS = [
    'amqp://test:test@localhost:32783/test',
    'amqp://test:test@localhost:32789/test',
]


async def main(loop):
    message_count = 0

    # connection = await custom_connect(
    #     urls=URLS, loop=loop
    # )

    connection = await round_robust.custom_connect(urls=URLS, loop=loop)

    queue_name = "test_queue"

    async with connection:
        # Creating channel
        channel = await connection.channel()

        # Declaring queue
        queue = await channel.declare_queue(queue_name, auto_delete=True)

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    message_count += 1
                    print(message_count, message.body)

                    # if queue.name in message.body.decode():
                    #     break


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    loop.close()
