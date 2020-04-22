import asyncio
import logging
from functools import partial

import aiormq
from aio_pika import RobustConnection
from aio_pika.connection import Connection
from aiormq.tools import censor_url
from yarl import URL


logger = logging.getLogger(__name__)


class CustomRobustConnection(RobustConnection):
    """ Robust custom connection """

    DEFAULT_RECONNECT_INTERVAL = 2

    def __init__(self, urls, loop=None, **kwargs):
        """ Меняем инициализацию на list urls

        """
        super().__init__(URL(urls[0]), loop=loop, **kwargs)
        # self.reconnect_interval = int(
        #     self._get_connection_argument('reconnect_interval', self.DEFAULT_RECONNECT_INTERVAL)
        # )
        self.__channels = set()
        self._on_connection_lost_callbacks = set()
        self._on_reconnect_callbacks = set()
        self._closed = False

        self.urls = urls
        self.url_next = 0
        self.first_connect = True

    def get_url(self):
        """ Получение следующего по очереди uri для коннекта

        :return: str
        """
        if self.url_next > (len(self.urls) - 1):
            self.url_next = 0
        self.url = URL(self.urls[self.url_next])
        self.url_next += 1
        return self.url

    async def connect(self, timeout=None):
        """ Переопределяем метод для использования кастомной функции get_url

        """
        url = self.get_url()
        logger.info(f'Connecting to RMQ {censor_url(url)}')
        self.connection = await asyncio.wait_for(
            aiormq.connect(url), timeout=timeout, loop=self.loop
        )  # type: aiormq.Connection

        self.connection.closing.add_done_callback(partial(self._on_connection_close, self.connection))
        self.first_connect = False
        logger.info(f'Successful connected to RMQ {censor_url(url)}')

    async def reconnect(self):
        if not self.first_connect:
            logger.warning('Connection to RMQ lost! Reconnecting')
        try:
            # Calls `_on_connection_lost` in case of errors
            await self.connect(self.DEFAULT_RECONNECT_INTERVAL)
            # await self._on_reconnect()
        except Exception:
            logger.exception('Connection attempt error')
            self.loop.call_later(self.reconnect_interval, lambda: self.loop.create_task(self.reconnect()))


async def custom_connect(
    urls: list = None,
    *,
    host: str = 'localhost',
    port: int = 5672,
    login: str = 'guest',
    password: str = 'guest',
    virtualhost: str = '/',
    ssl: bool = False,
    loop=None,
    ssl_options: dict = None,
    connection_class=CustomRobustConnection,
    **kwargs,
) -> Connection:
    """ Кастомный коннект """
    if len(urls) == 0:
        kw = kwargs
        kw.update(ssl_options or {})

        urls[0] = URL.build(
            scheme='amqps' if ssl else 'amqp',
            host=host,
            port=port,
            user=login,
            password=password,
            # yarl >= 1.3.0 requires path beginning with slash
            path="/" + virtualhost,
            query=kw,
        )

    connection = connection_class(urls, loop=loop)
    await connection.reconnect()
    return connection
