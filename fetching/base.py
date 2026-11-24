# -*- coding: utf-8 -*-
"""
fetching/base.py — 抓取器通用接口与数据模型
"""
from dataclasses import dataclass
from typing import Optional


@dataclass
class PageResponse:
    """统一页面响应模型"""
    url: str
    status: int
    text: str
    backend: str              # "scrapling" | "httpx"
    fingerprint: str = ""     # 使用的 impersonate 或 UA
    proxy: str = ""           # 使用的代理
    error: str = ""           # 异常或失败描述


class BaseBackend:
    """抓取后端基类"""
    def get(self, url: str) -> PageResponse:
        raise NotImplementedError

    def close(self) -> None:
        pass
