from dataclasses import dataclass
from typing import Optional


@dataclass
class ExtensionItem:
    extension_type: int
    data: bytes
    metadata: Optional[bytes] = None
    ignore: bool = False
