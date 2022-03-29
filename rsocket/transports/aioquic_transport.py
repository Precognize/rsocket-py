import asyncio
from contextlib import asynccontextmanager

from aioquic.asyncio import QuicConnectionProtocol, connect, serve
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import QuicEvent, StreamDataReceived

from rsocket.frame import Frame
from rsocket.helpers import wrap_transport_exception
from rsocket.logger import logger
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.abstract_messaging import AbstractMessagingTransport
from rsocket.transports.transport import Transport


@asynccontextmanager
async def rsocket_connect(host: str, port: int, configuration: QuicConfiguration = None) -> Transport:
    if configuration is None:
        configuration = QuicConfiguration(
            is_client=True
        )

    async with connect(
            host,
            port,
            configuration=configuration,
            create_protocol=RSocketQuicProtocol,
    ) as client:
        yield RSocketQuicTransport(client)


def rsocket_serve(host: str,
                  port: int,
                  configuration: QuicConfiguration = None,
                  on_server_create=None,
                  **kwargs):
    if configuration is None:
        configuration = QuicConfiguration(
            is_client=False
        )

    def protocol_factory(*protocol_args, **protocol_kwargs):
        protocol = RSocketQuicProtocol(*protocol_args, **protocol_kwargs)
        server = RSocketServer(RSocketQuicTransport(protocol), **kwargs)

        if on_server_create is not None:
            on_server_create(server)

        return protocol

    return serve(
        host,
        port,
        create_protocol=protocol_factory,
        configuration=configuration)


class RSocketQuicProtocol(QuicConnectionProtocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.frame_queue = asyncio.Queue()
        self._stream_id = self._quic.get_next_available_stream_id()

    async def query(self, frame: Frame) -> None:
        data = frame.serialize()
        self._quic.send_stream_data(self._stream_id, data, end_stream=False)
        self.transmit()

    def quic_event_received(self, event: QuicEvent) -> None:
        if isinstance(event, StreamDataReceived):
            self.frame_queue.put_nowait(event.data)


class RSocketQuicTransport(AbstractMessagingTransport):
    def __init__(self, quic_protocol: RSocketQuicProtocol):
        super().__init__()
        self._quic_protocol = quic_protocol
        self._incoming_bytes_queue = quic_protocol.frame_queue
        self._listener = asyncio.create_task(self.incoming_data_listener())

    async def send_frame(self, frame: Frame):
        with wrap_transport_exception():
            await self._quic_protocol.query(frame)

    async def incoming_data_listener(self):
        with wrap_transport_exception():
            try:
                while True:
                    data = await self._incoming_bytes_queue.get()

                    async for frame in self._frame_parser.receive_data(data, 0):
                        self._incoming_frame_queue.put_nowait(frame)
            except asyncio.CancelledError:
                logger().debug('Asyncio task canceled: incoming_data_listener')

    async def close(self):
        self._listener.cancel()
        self._quic_protocol.close()