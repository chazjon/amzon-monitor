# -*- coding: utf-8 -*-
"""
fetching/chain.py — 链式降级抓取后端
"""
from typing import List
from .base import BaseBackend, PageResponse


class ChainBackend(BaseBackend):
    def __init__(self, backends: List[BaseBackend]):
        if not backends:
            raise ValueError("ChainBackend requires at least one backend")
        self.backends = backends

    def get(self, url: str) -> PageResponse:
        last_resp = None
        for b in self.backends:
            resp = b.get(url)
            last_resp = resp
            # status > 0 表示 HTTP 通信成功
            if resp.status > 0:
                return resp
        return last_resp

    def close(self) -> None:
        for b in self.backends:
            b.close()
