from collections import OrderedDict
from typing import Generator

from rsocket.frame import Frame


class FrameCache:
    def __init__(self):
        self._frame_by_position = OrderedDict()
        self._last_position = 0

    def store(self, frame: Frame) -> Frame:
        self._last_position += 1
        self._frame_by_position[self._last_position] = frame
        return frame
 
    def get_frames(self, first_position: int) -> Generator[Frame, None, None]:
        for position in range(first_position, self._last_position):
            yield self._frame_by_position[position]

    def clear_until_position(self, last_position: int):
        first_position = next(self._frame_by_position.keys())

        for position in range(first_position, last_position):
            del self._frame_by_position[position]
