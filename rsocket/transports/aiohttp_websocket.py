import asyncio
from contextlib import asynccontextmanager

import aiohttp
from aiohttp import web

from rsocket.frame import Frame
from rsocket.helpers import wrap_transport_exception, single_transport_provider
from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.abstract_websocket import AbstractWebsocketTransport


@asynccontextmanager
async def websocket_client(url, *args, **kwargs) -> RSocketClient:
    async with RSocketClient(single_transport_provider(TransportAioHttpClient(url)),
                             *args, **kwargs) as client:
        yield client


def websocket_handler_factory(*args, on_server_create=None, **kwargs):
    async def websocket_handler(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        transport = TransportAioHttpWebsocket(ws)
        server = RSocketServer(transport, *args, **kwargs)

        if on_server_create is not None:
            on_server_create(server)

        await transport.handle_incoming_ws_messages()
        return ws

    return websocket_handler


class TransportAioHttpClient(AbstractWebsocketTransport):

    def __init__(self, url):
        super().__init__()
        self._url = url

    async def connect(self):
        self._session = aiohttp.ClientSession()
        self._ws_context = self._session.ws_connect(self._url)
        self._ws = await self._ws_context.__aenter__()
        self._message_handler = asyncio.create_task(self.handle_incoming_ws_messages())

    async def handle_incoming_ws_messages(self):
        with wrap_transport_exception():
            async for msg in self._ws:
                if msg.type == aiohttp.WSMsgType.BINARY:
                    async for frame in self._frame_parser.receive_data(msg.data, 0):
                        self._incoming_frame_queue.put_nowait(frame)

    async def send_frame(self, frame: Frame):
        with wrap_transport_exception():
            await self._ws.send_bytes(frame.serialize())

    async def close(self):
        await self._ws_context.__aexit__(None, None, None)
        await self._session.__aexit__(None, None, None)
        self._message_handler.cancel()
        await self._message_handler


class TransportAioHttpWebsocket(AbstractWebsocketTransport):
    def __init__(self, websocket):
        super().__init__()
        self._ws = websocket

    async def _message_generator(self):
        with wrap_transport_exception():
            async for msg in self._ws:
                if msg.type == aiohttp.WSMsgType.BINARY:
                    yield msg.data

    async def handle_incoming_ws_messages(self):
        async for message in self._message_generator():
            async for frame in self._frame_parser.receive_data(message, 0):
                self._incoming_frame_queue.put_nowait(frame)

    async def send_frame(self, frame: Frame):
        with wrap_transport_exception():
            await self._ws.send_bytes(frame.serialize())

    async def close(self):
        await self._ws.close()
