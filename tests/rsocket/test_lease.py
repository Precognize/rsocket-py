import asyncio
from datetime import timedelta

import pytest

from reactivestreams.subscriber import Subscriber
from rsocket.exceptions import RSocketRejected
from rsocket.lease import SingleLeasePublisher, DefinedLease
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler


class PeriodicalLeasePublisher(SingleLeasePublisher):

    async def subscribe(self, subscriber: Subscriber):
        asyncio.ensure_future(self._subscribe_loop(subscriber))

    async def _subscribe_loop(self, subscriber: Subscriber):
        while True:
            await asyncio.sleep(self.wait_between_leases.total_seconds())

            await subscriber.on_next(DefinedLease(
                maximum_request_count=self.maximum_request_count,
                maximum_lease_time=self.maximum_lease_time
            ))


@pytest.mark.asyncio
async def test_request_response_with_server_side_lease_works(lazy_pipe):
    class Handler(BaseRequestHandler):
        async def request_response(self, request: Payload):
            future = asyncio.Future()
            future.set_result(Payload(b'data: ' + request.data,
                                      b'meta: ' + request.metadata))
            return future

    async with lazy_pipe(client_arguments={'honor_lease': True},
                         server_arguments={'handler_factory': Handler,
                                           'lease_publisher': SingleLeasePublisher(
                                               maximum_lease_time=timedelta(seconds=3)
                                           )}) as (server, client):
        for x in range(2):
            response = await client.request_response(Payload(b'dog', b'cat'))
            assert response == Payload(b'data: dog', b'meta: cat')


@pytest.mark.asyncio
async def test_request_response_with_client_and_server_side_lease_works(lazy_pipe):
    class Handler(BaseRequestHandler):
        async def request_response(self, request: Payload):
            future = asyncio.Future()
            future.set_result(Payload(b'data: ' + request.data,
                                      b'meta: ' + request.metadata))
            return future

    async with lazy_pipe(client_arguments={'honor_lease': True,
                                           'handler_factory': Handler,
                                           'lease_publisher': PeriodicalLeasePublisher(
                                               maximum_request_count=2,
                                               maximum_lease_time=timedelta(seconds=3),
                                               wait_between_leases=timedelta(seconds=2)
                                           )},
                         server_arguments={'honor_lease': True,
                                           'handler_factory': Handler,
                                           'lease_publisher': PeriodicalLeasePublisher(
                                               maximum_request_count=2,
                                               maximum_lease_time=timedelta(seconds=3),
                                               wait_between_leases=timedelta(seconds=2)
                                           )}) as (server, client):
        for x in range(3):
            response = await client.request_response(Payload(b'dog', b'cat'))
            assert response == Payload(b'data: dog', b'meta: cat')

        for x in range(3):
            response = await server.request_response(Payload(b'dog', b'cat'))
            assert response == Payload(b'data: dog', b'meta: cat')


@pytest.mark.asyncio
async def test_request_response_with_lease_too_many_requests(lazy_pipe):
    class Handler(BaseRequestHandler):
        async def request_response(self, request: Payload):
            future = asyncio.Future()
            future.set_result(Payload(b'data: ' + request.data,
                                      b'meta: ' + request.metadata))
            return future

    async with lazy_pipe(client_arguments={'honor_lease': True},
                         server_arguments={'handler_factory': Handler,
                                           'lease_publisher': SingleLeasePublisher(
                                               maximum_request_count=2
                                           )}) as (server, client):
        for x in range(2):
            response = await client.request_response(Payload(b'dog', b'cat'))
            assert response == Payload(b'data: dog', b'meta: cat')

        with pytest.raises(asyncio.exceptions.TimeoutError):
            await asyncio.wait_for(client.request_response(Payload(b'invalid request')), 3)


@pytest.mark.asyncio
async def test_request_response_with_lease_client_side_exception_requests_late(lazy_pipe):
    class Handler(BaseRequestHandler):
        async def request_response(self, request: Payload):
            future = asyncio.Future()
            future.set_result(Payload(b'data: ' + request.data,
                                      b'meta: ' + request.metadata))
            return future

    async with lazy_pipe(client_arguments={'honor_lease': True},
                         server_arguments={'handler_factory': Handler,
                                           'lease_publisher': SingleLeasePublisher(
                                               maximum_lease_time=timedelta(seconds=3)
                                           )}) as (server, client):
        for x in range(2):
            response = await client.request_response(Payload(b'dog', b'cat'))
            assert response == Payload(b'data: dog', b'meta: cat')

        await asyncio.sleep(5)

        with pytest.raises(asyncio.exceptions.TimeoutError):
            await asyncio.wait_for(client.request_response(Payload(b'invalid request')), 3)


@pytest.mark.asyncio
async def test_server_rejects_all_requests_if_lease_not_supported(lazy_pipe):
    async with lazy_pipe(client_arguments={'honor_lease': True}) as (server, client):
        with pytest.raises(asyncio.exceptions.TimeoutError):
            await asyncio.wait_for(client.request_response(Payload(b'invalid request')), 3)


@pytest.mark.asyncio
@pytest.mark.skip(reason='TODO')
async def test_request_response_with_lease_server_side_exception(lazy_pipe):
    class Handler(BaseRequestHandler):
        async def request_response(self, request: Payload):
            future = asyncio.Future()
            future.set_result(Payload(b'data: ' + request.data,
                                      b'meta: ' + request.metadata))
            return future

    async with lazy_pipe(client_arguments={'honor_lease': True},
                         server_arguments={'handler_factory': Handler,
                                           'lease_publisher': SingleLeasePublisher(
                                               maximum_request_count=2
                                           )}) as (server, client):
        for x in range(2):
            response = await client.request_response(Payload(b'dog', b'cat'))
            assert response == Payload(b'data: dog', b'meta: cat')

        with pytest.raises(RSocketRejected):
            await client.request_response(Payload(b'invalid request'))