import abc
from typing import Optional

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import Subscription
from rsocket.frame import CancelFrame, ErrorFrame, RequestNFrame, \
    PayloadFrame, Frame, error_frame_to_exception
from rsocket.payload import Payload
from rsocket.rsocket_interface import RSocketInterface
from rsocket.streams.stream_handler import StreamHandler


class RequestChannelCommon(StreamHandler, Publisher, Subscription, metaclass=abc.ABCMeta):
    class StreamSubscriber(Subscriber):
        def __init__(self, stream_id: int, socket, requester: 'RequestChannelCommon'):
            super().__init__()
            self._stream_id = stream_id
            self._socket = socket
            self._requester = requester
            self.subscription = None

        def on_next(self, value, is_complete=False):
            self._socket.send_payload(
                self._stream_id, value, complete=is_complete)

            if is_complete:
                self._requester.mark_completed_and_finish(sent=True)

        def on_complete(self):
            self._socket.send_payload(
                self._stream_id, Payload(), complete=True, is_next=False)
            self._requester.mark_completed_and_finish(sent=True)

        def on_error(self, exception):
            self._socket.send_error(self._stream_id, exception)
            self._requester.mark_completed_and_finish(sent=True)

        def on_subscribe(self, subscription):
            # noinspection PyAttributeOutsideInit
            self.subscription = subscription

    def __init__(self, socket: RSocketInterface, remote_publisher: Optional[Publisher] = None):
        super().__init__(socket)
        self.remote_subscriber = None
        self._sent_complete = False
        self._received_complete = False
        self._remote_publisher = remote_publisher

    def setup(self):
        self.subscriber = self.StreamSubscriber(self.stream_id, self.socket, self)

        if self._remote_publisher is not None:
            self._remote_publisher.subscribe(self.subscriber)

    def frame_received(self, frame: Frame):
        if isinstance(frame, CancelFrame):
            self.subscriber.subscription.cancel()
            self._finish_stream()
        elif isinstance(frame, RequestNFrame):
            self.subscriber.subscription.request(frame.request_n)

        elif isinstance(frame, PayloadFrame):
            if frame.flags_next:
                self.remote_subscriber.on_next(Payload(frame.data, frame.metadata),
                                               is_complete=frame.flags_complete)
            elif frame.flags_complete:
                self.remote_subscriber.on_complete()

            if frame.flags_complete:
                self.mark_completed_and_finish(received=True)
        elif isinstance(frame, ErrorFrame):
            self.remote_subscriber.on_error(error_frame_to_exception(frame))
            self.mark_completed_and_finish(received=True)

    def _complete_remote_subscriber(self):
        if self.remote_subscriber is not None:
            self.remote_subscriber.on_complete()

        self.mark_completed_and_finish(received=True)

    def mark_completed_and_finish(self, received=None, sent=None):
        if received:
            self._received_complete = True
        if sent:
            self._sent_complete = True
        self._finish_if_both_closed()

    def _finish_if_both_closed(self):
        if self._received_complete and self._sent_complete:
            self._finish_stream()

    def subscribe(self, subscriber: Subscriber):
        if subscriber is not None:
            self.remote_subscriber = subscriber
            self.remote_subscriber.on_subscribe(self)
        else:
            self.mark_completed_and_finish(received=True)

    def cancel(self):
        self.send_cancel()

    def request(self, n: int):
        self.send_request_n(n)
