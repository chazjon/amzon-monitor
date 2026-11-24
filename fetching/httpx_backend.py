# -*- coding: utf-8 -*-
"""
fetching/httpx_backend.py — 基于 httpx 的原有实现适配器（供兜底与回滚）
"""
import random
from typing import List, Optional
import httpx

from .base import BaseBackend, PageResponse


class HttpxBackend(BaseBackend):
    def __init__(
        self,
        user_agents: List[str],
        timeout: int = 25,
        proxy: Optional[str] = None,
    ):
        self.user_agents = user_agents
        self.timeout = timeout
        self.proxy = proxy
        self.client = httpx.Client(
            follow_redirects=True,
            trust_env=True,
            proxy=proxy,
            timeout=timeout,
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "DNT": "1",
            },
        )
        self.client.cookies.set("lc-main", "en_US", domain=".amazon.com")
        self.client.cookies.set("i18n-prefs", "USD", domain=".amazon.com")

    def get(self, url: str) -> PageResponse:
        ua = random.choice(self.user_agents)
        self.client.headers["User-Agent"] = ua
        try:
            resp = self.client.get(url)
            return PageResponse(
                url=str(resp.url),
                status=resp.status_code,
                text=resp.text,
                backend="httpx",
                fingerprint=ua,
                proxy=self.proxy or "",
            )
        except Exception as e:
            return PageResponse(
                url=url,
                status=0,
                text="",
                backend="httpx",
                fingerprint=ua,
                proxy=self.proxy or "",
                error=f"{type(e).__name__}: {e}",
            )

    def close(self) -> None:
        self.client.close()
