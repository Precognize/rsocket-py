from datetime import timedelta
from typing import Optional

from expiring_dict import ExpiringDict

from rsocket.rsocket_interface import RSocketInterface
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.transport import Transport

_default = object()


class ServerRegistry:
    def __init__(self,
                 handler_factory,
                 server_factory=_default,
                 server_expiration_ttl=timedelta(hours=1)):
        self._handler_factory = handler_factory
        self._server_factory = server_factory
        self._server_expiration_ttl = server_expiration_ttl
        self._server_by_resume_token = ExpiringDict(
            self._server_expiration_ttl.total_seconds())

    def register_server(self, server: RSocketInterface):
        self._server_by_resume_token[server.resume_token] = server

    def get_or_create_server(self, transport: Transport, resume_token: Optional[str] = None) -> RSocketInterface:
        existing_server = self._server_by_resume_token.get(resume_token)

        if existing_server is not None:
            return existing_server.set_transport(transport)

        return self._new_server(transport)

    def _new_server(self, transport: Transport) -> RSocketInterface:
        return RSocketServer(transport, handler_factory=self._handler_factory)
