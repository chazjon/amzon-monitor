# -*- coding: utf-8 -*-
"""
fetching/scrapling_backend.py — 基于 Scrapling 的原生 Fetcher 适配器
"""
import random
from typing import List, Optional
from scrapling.fetchers import Fetcher

from .base import BaseBackend, PageResponse

DEFAULT_IMPERSONATE = [
    "chrome",
    "chrome110",
    "chrome120",
    "safari17_0",
]


class ScraplingBackend(BaseBackend):
    def __init__(
        self,
        impersonate_list: Optional[List[str]] = None,
        timeout: int = 30,
        proxy: Optional[str] = None,
    ):
        self.impersonate_list = impersonate_list or list(DEFAULT_IMPERSONATE)
        self.timeout = timeout
        self.proxy = proxy

    def get(self, url: str) -> PageResponse:
        imp = random.choice(self.impersonate_list)
        try:
            r = Fetcher.get(
                url,
                impersonate=imp,
                timeout=self.timeout,
                proxy=self.proxy,
                stealthy_headers=True,
            )
            html = r.html_content if hasattr(r, "html_content") else str(r)
            return PageResponse(
                url=str(r.url),
                status=r.status,
                text=html,
                backend="scrapling",
                fingerprint=imp,
                proxy=self.proxy or "",
            )
        except Exception as e:
            return PageResponse(
                url=url,
                status=0,
                text="",
                backend="scrapling",
                fingerprint=imp,
                proxy=self.proxy or "",
                error=f"{type(e).__name__}: {e}",
            )

    def close(self) -> None:
        pass
