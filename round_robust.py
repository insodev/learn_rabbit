import asyncio
import logging
from logging import getLogger
from typing import List
from typing import Type

import aiormq
from aio_pika import RobustConnection
from aio_pika.connection import ConnectionType
from aio_pika.types import TimeoutType
from yarl import URL

logger = logging.getLogger(__name__)
log = getLogger(__name__)


class RoundRobustConnection(RobustConnection):
    def __init__(self, urls, *, loop=None, **kwargs):
        first_url = urls[0]
        self._urls: list = [URL(url) for url in urls]
        self._index_url = 0

        super().__init__(first_url, loop, **kwargs)

    @property
    def url(self):
        _url = self._urls[self._index_url]
        logger.debug(f'get url {(self._index_url, _url)}')
        return _url

    @url.setter
    def url(self, value):

        logger.debug(f'url.setter before : {self._urls}')

        if value not in self._urls:
            self._index_url = len(self._urls)
            self._urls.append(value)
        else:
            self._index_url = self._urls.index(value)

        logger.debug(f'url.setter after : {self._urls}')

    def _change_url(self):
        current = self.url
        self._index_url = (self._index_url + 1) % len(self._urls)
        logger.debug(f'change url from {current} to {self.url}')

    async def _make_connection(self, **kwargs) -> aiormq.Connection:
        logger.debug('_make_connection ->>> change url if wanna')
        if not self.fail_fast:
            self._change_url()
        return await super()._make_connection(**kwargs)


async def connect_round_robust(
    urls: List[str],
    *,
    loop: asyncio.AbstractEventLoop = None,
    timeout: TimeoutType = None,
    connection_class: Type[ConnectionType] = RoundRobustConnection,
) -> ConnectionType:

    logger.debug('First connect !')

    connection = connection_class(urls, loop=loop)

    await connection.connect(timeout=timeout, loop=loop)
    return connection


custom_connect = connect_round_robust

__all__ = (
    'RoundRobustConnection',
    'connect_round_robust',
    'custom_connect',
)
