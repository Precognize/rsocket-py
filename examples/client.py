import asyncio
import logging

from reactivestreams.subscriber import Subscriber
from rsocket.payload import Payload
from rsocket.rsocket_client import RSocketClient
from rsocket.transports.tcp import TransportTCP


class StreamSubscriber(Subscriber):

    async def on_next(self, value, is_complete=False):
        await self.subscription.request(1)

    def on_subscribe(self, subscription):
        self.subscription = subscription


async def main():
    connection = await asyncio.open_connection('localhost', 6565)

    async with RSocketClient(TransportTCP(*connection)) as client:
        payload = Payload(b'%Y-%m-%d %H:%M:%S')

        async def run_request_response():
            try:
                while True:
                    result = await client.request_response(payload)
                    logging.info('Response: {}'.format(result.data))
                    await asyncio.sleep(1)
            except asyncio.CancelledError:
                pass

        task = asyncio.create_task(run_request_response())

        await asyncio.sleep(5)
        task.cancel()
        await task


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main())
