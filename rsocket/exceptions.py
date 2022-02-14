from typing import Optional

from rsocket.error_codes import ErrorCode


class RSocketError(Exception):
    pass


class RSocketApplicationError(RSocketError):
    pass


class RSocketStreamAllocationFailure(RSocketError):
    pass


class RSocketValueErrorException(RSocketError):
    pass


class RSocketProtocolException(RSocketError):
    def __init__(self, error_code: ErrorCode, data: Optional[str] = None):
        self.error_code = error_code
        self.data = data

    def __str__(self) -> str:
        return 'RSocket error %s(%s): "%s"' % (self.error_code.name, self.error_code.value, self.data or '')


class RSocketFrameFragmentDifferentType(RSocketError):
    pass
