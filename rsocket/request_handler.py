import asyncio
from abc import ABCMeta, abstractmethod
from asyncio import Future
from typing import Tuple, Optional

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber
from rsocket.error_codes import ErrorCode
from rsocket.extensions.composite_metadata import CompositeMetadata
from rsocket.logger import logger
from rsocket.payload import Payload


class RequestHandler(metaclass=ABCMeta):

    def __init__(self, socket):
        super().__init__()
        self.socket = socket

    @abstractmethod
    async def on_setup(self,
                       data_encoding: bytes,
                       metadata_encoding: bytes,
                       payload: Payload):
        ...

    @abstractmethod
    async def on_metadata_push(self, metadata: Payload):
        ...

    @abstractmethod
    async def request_channel(self,
                              payload: Payload
                              ) -> Tuple[Optional[Publisher], Optional[Subscriber]]:
        """
        Bi-Directional communication.  A publisher on each end is connected
        to a subscriber on the other end.
        """

    @abstractmethod
    async def request_fire_and_forget(self, payload: Payload):
        ...

    @abstractmethod
    async def request_response(self, payload: Payload) -> asyncio.Future:
        ...

    @abstractmethod
    async def request_stream(self, payload: Payload) -> Publisher:
        ...

    @abstractmethod
    async def on_error(self, error_code: ErrorCode, payload: Payload):
        ...

    def _parse_composite_metadata(self, metadata: bytes) -> CompositeMetadata:
        composite_metadata = CompositeMetadata()
        composite_metadata.parse(metadata)
        return composite_metadata


class BaseRequestHandler(RequestHandler):

    async def on_setup(self,
                       data_encoding: bytes,
                       metadata_encoding: bytes,
                       payload: Payload):
        """Nothing to do on setup by default"""

    async def request_channel(self, payload: Payload) -> Tuple[Optional[Publisher], Optional[Subscriber]]:
        raise RuntimeError('Not implemented')

    async def request_fire_and_forget(self, payload: Payload):
        """The requester isn't listening for errors.  Nothing to do."""

    async def on_metadata_push(self, payload: Payload):
        """Nothing by default"""

    async def request_response(self, payload: Payload) -> Future:
        future = asyncio.Future()
        future.set_exception(RuntimeError('Not implemented'))
        return future

    async def request_stream(self, payload: Payload) -> Publisher:
        raise RuntimeError('Not implemented')

    async def on_error(self, error_code: ErrorCode, payload: Payload):
        logger().error('Error: %s, %s', error_code, payload)
