# -*- coding: utf-8 -*-
from .base import BaseBackend, PageResponse
from .httpx_backend import HttpxBackend
from .scrapling_backend import ScraplingBackend
from .chain import ChainBackend

__all__ = [
    "BaseBackend",
    "PageResponse",
    "HttpxBackend",
    "ScraplingBackend",
    "ChainBackend",
]
