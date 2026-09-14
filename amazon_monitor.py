#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
amazon_monitor.py — 亚马逊商品评分/评论数每日监控

每天定时由 TRAE 定时任务触发，完整流程：
  1. 读取飞书表格两个工作表 A 列的 ASIN（行号映射）
  2. 抓取亚马逊美国站商品页 https://www.amazon.com/dp/{ASIN}
     - 相邻请求随机间隔 1.3~3.0 秒，UA 池轮换，Session 保持 Cookie
     - 失败重试最多 3 次（退避 5/10/15 秒 + 随机抖动）
  3. 结果写回同一表格：每个商品一行、每天一列（最新日期插在 B 列，历史保留）
     单元格格式沿用现有约定："4.8 (97)"；无评分写 "-"；最终失败写 "抓取失败"
  4. 数据写回后，把当天与前一天对比，值不一致的当天单元格标黄（#FFFF00）
  5. stdout 输出运行摘要，退出码 0=正常结束（允许部分商品失败），2=致命错误

用法：
  python3 amazon_monitor.py            # 正常运行（抓取 + 写回 + 差异标黄）
  python3 amazon_monitor.py --dry-run  # 抓取并打印结果，不写回/不标黄
  python3 amazon_monitor.py --limit N  # 只处理前 N 个去重后的 ASIN（调试）
  python3 amazon_monitor.py --url <表> # 覆盖目标飞书表格 URL（如测试副本表）

依赖：httpx（pip install httpx）、lark-cli（已登录飞书用户身份）
网络：自动遵循环境变量 HTTP_PROXY / HTTPS_PROXY（httpx trust_env）
"""

import csv
import io
import json
import os
import random
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import httpx

# ============================== 配置区 ==============================

SPREADSHEET_URL = "https://tcn4m7idpero.feishu.cn/sheets/HpK9swqZwhiCKztODP3c17twn4f"
SHEET_NAMES = ["监控数据", "戒指监测2"]

# 当前实际使用的表格 URL；默认生产表，也可用 --url 覆盖（用于测试副本表）
_ACTIVE_URL = SPREADSHEET_URL

AMAZON_DP_URL = "https://www.amazon.com/dp/{asin}"

REQUEST_INTERVAL_MIN = 1.3      # 相邻商品请求最小间隔（秒）
REQUEST_INTERVAL_MAX = 3.0      # 相邻商品请求最大间隔（秒）
MAX_RETRIES = 3                 # 单链接最大尝试次数
RETRY_BACKOFF = [5, 10, 15]     # 第 1/2/3 次重试前的退避秒数（另加 0~2 秒抖动）
REQUEST_TIMEOUT = 25            # 单次 HTTP 超时（秒）

TZ = ZoneInfo("Asia/Shanghai")
TODAY = datetime.now(TZ).strftime("%Y-%m-%d")

FAIL_TEXT = "抓取失败"
NO_RATING_TEXT = "-"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:129.0) Gecko/20100101 Firefox/129.0",
]

LARK_ENV = {
    **os.environ,
    "LARKSUITE_CLI_NO_UPDATE_NOTIFIER": "1",
    "LARKSUITE_CLI_NO_SKILLS_NOTIFIER": "1",
}

# ============================== 解析正则 ==============================

RATING_RE = re.compile(r'id="acrPopover"[^>]*title="([0-9.]+) out of 5 stars"')
RATING_RE_FALLBACK = re.compile(r'([0-9]\.[0-9]) out of 5 stars')
REVIEWS_RES = [
    re.compile(r'id="acrCustomerReviewText"[^>]*>\s*\(?([\d,]+)\)?\s*<'),
    re.compile(r'id="acrCustomerReviewText"[^>]*aria-label="([\d,]+)', re.I),
    re.compile(r'aria-label="([\d,]+)\s+(?:Reviews?|ratings?)"', re.I),
]
CAPTCHA_RE = re.compile(
    r'(Enter the characters you see below|Type the characters you see in this image'
    r'|Robot Check|validateCaptcha|are not a robot)', re.I)
PRODUCT_TITLE_RE = re.compile(r'id="productTitle"')
DATE_PREFIX_RE = re.compile(r'^\s*(\d{4}-\d{2}-\d{2})')

# ============================== 数据结构 ==============================


@dataclass
class SheetInfo:
    name: str
    header: list = field(default_factory=list)          # 第 1 行表头
    asin_rows: dict = field(default_factory=dict)       # asin -> 行号
    today_col_idx: Optional[int] = None                 # 今日列 0 基列号（无则 None）


@dataclass
class ScrapeResult:
    asin: str
    status: str                          # "ok" | "no_rating" | "failed"
    rating: Optional[str] = None
    reviews: Optional[str] = None
    attempts: int = 0
    error: str = ""

    def cell_text(self) -> str:
        if self.status == "ok":
            return f"{self.rating} ({self.reviews})"
        if self.status == "no_rating":
            return NO_RATING_TEXT
        return FAIL_TEXT


class FatalError(Exception):
    pass

# ============================== 飞书读写 ==============================


def lark(args):
    """调用 lark-cli sheets 子命令，返回 data 字段；失败抛 FatalError。"""
    cmd = ["lark-cli", "sheets", *args, "--url", _ACTIVE_URL]
    try:
        p = subprocess.run(cmd, capture_output=True, text=True,
                           env=LARK_ENV, timeout=180)
    except FileNotFoundError:
        raise FatalError("未找到 lark-cli，请确认已安装并在 PATH 中")
    except subprocess.TimeoutExpired:
        raise FatalError(f"lark-cli 调用超时: {' '.join(args)}")
    if p.returncode != 0:
        raise FatalError(f"lark-cli 失败 [{' '.join(args)}]: {p.stderr.strip()[:400]}")
    try:
        env = json.loads(p.stdout)
    except json.JSONDecodeError:
        raise FatalError(f"lark-cli 输出无法解析: {p.stdout[:300]}")
    if not env.get("ok"):
        raise FatalError(f"lark-cli 返回错误: {json.dumps(env.get('error'), ensure_ascii=False)[:400]}")
    return env.get("data", {})


def col_letter(idx0):
    """0 基列号 -> A1 列字母（0->A, 26->AA）。"""
    s, n = "", idx0 + 1
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def read_sheet_rows(sheet_name):
    """读取整个工作表，返回 {行号: [单元格字符串, ...]}。"""
    data = lark(["+csv-get", "--sheet-name", sheet_name])
    rows = {}
    for line in data.get("annotated_csv", "").splitlines():
        m = re.match(r"^\[row=(\d+)\]\s?(.*)$", line)
        if not m:
            continue
        rownum = int(m.group(1))
        body = m.group(2)
        cells = next(csv.reader(io.StringIO(body))) if body.strip() else []
        rows[rownum] = [c.strip() for c in cells]
    return rows


def analyze_sheet(name):
    """读取一个工作表，提取表头、ASIN 行号映射、今日列位置。"""
    rows = read_sheet_rows(name)
    header = rows.get(1, [])
    asin_rows = {}
    for rn in sorted(rows):
        if rn == 1:
            continue
        cells = rows[rn]
        if cells and cells[0]:
            asin_rows.setdefault(cells[0], rn)
    today_col_idx = None
    for i, h in enumerate(header):
        m = DATE_PREFIX_RE.match(h)
        if m and m.group(1) == TODAY:
            today_col_idx = i
            break
    return SheetInfo(name=name, header=header, asin_rows=asin_rows,
                     today_col_idx=today_col_idx)


def ensure_today_column(info, dry_run):
    """返回今日列的 A1 字母；不存在则在 B 列插入新列并写表头。"""
    if info.today_col_idx is not None:
        return col_letter(info.today_col_idx), False
    if not dry_run:
        lark(["+dim-insert", "--sheet-name", info.name,
              "--position", "B", "--count", "1"])
        lark(["+cells-set", "--sheet-name", info.name,
              "--range", "B1", "--cells", json.dumps([[{"value": TODAY}]])])
    return "B", True


def write_results(info, col, results, dry_run):
    """把抓取结果批量写入指定列。返回写入条数。"""
    writes = []
    for asin, rn in info.asin_rows.items():
        res = results.get(asin)
        if res is None:
            continue
        writes.append({"sheet_name": info.name, "range": f"{col}{rn}",
                       "cells": [[{"value": res.cell_text()}]]})
    if dry_run:
        return len(writes)
    for i in range(0, len(writes), 90):   # --writes 单次上限 100 条
        chunk = writes[i:i + 90]
        lark(["+cells-set", "--writes", json.dumps(chunk, ensure_ascii=False)])
    return len(writes)


def compare_and_highlight(sheet_name, dry_run):
    """对比当天与前一天的单元格值，不一致时把当天单元格标黄。

    返回 (标黄数, 描述)。以表头中的日期为准定位今日列与前一日列，
    仅当两个对应的单元格当前都有值且值不同才标黄（空值/新 ASIN 不标）。
    """
    rows = read_sheet_rows(sheet_name)
    header = rows.get(1, [])
    yday = (datetime.now(TZ) - timedelta(days=1)).strftime("%Y-%m-%d")

    def find_date_col(target):
        for i, h in enumerate(header):
            m = DATE_PREFIX_RE.match(h)
            if m and m.group(1) == target:
                return i
        return None

    today_col = find_date_col(TODAY)
    yday_col = find_date_col(yday)
    if today_col is None or yday_col is None:
        return 0, f"对比跳过（今日列={today_col is not None}，前一日列={yday_col is not None}）"

    diff_cells = []
    for rn in sorted(rows):
        if rn == 1:
            continue
        cells = rows[rn]
        if len(cells) <= today_col or len(cells) <= yday_col:
            continue
        t, y = cells[today_col], cells[yday_col]
        if t and y and t != y:
            diff_cells.append(f"{col_letter(today_col)}{rn}")

    if diff_cells and not dry_run:
        for rng in diff_cells:
            lark(["+cells-set-style", "--sheet-name", sheet_name,
                  "--range", rng, "--background-color", "#FFFF00"])
    action = "将标黄" if dry_run else "已标黄"
    return len(diff_cells), f"对比前一日完成：{len(diff_cells)} 处不一致{action}"


def verify_sheets_exist():
    """确认两个工作表都存在于工作簿中。"""
    data = lark(["+workbook-info"])
    existing = {s["sheet_name"] for s in data.get("sheets", [])}
    missing = [n for n in SHEET_NAMES if n not in existing]
    if missing:
        raise FatalError(f"工作簿中找不到工作表: {missing}；现有: {sorted(existing)}")

# ============================== 亚马逊抓取 ==============================


def parse_product_page(html):
    """从商品页 HTML 提取 (评分, 评论数)；找不到返回 (None, None)。"""
    rating = None
    m = RATING_RE.search(html) or RATING_RE_FALLBACK.search(html)
    if m:
        rating = m.group(1)
    reviews = None
    for rx in REVIEWS_RES:
        mr = rx.search(html)
        if mr:
            reviews = mr.group(1).replace(",", "")
            break
    return rating, reviews


def make_client():
    client = httpx.Client(
        follow_redirects=True,
        trust_env=True,           # 自动使用 HTTP_PROXY / HTTPS_PROXY
        timeout=REQUEST_TIMEOUT,
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
    client.cookies.set("lc-main", "en_US", domain=".amazon.com")
    client.cookies.set("i18n-prefs", "USD", domain=".amazon.com")
    return client


def fetch_product(client, asin):
    """抓取单个 ASIN，含重试。返回 ScrapeResult。"""
    url = AMAZON_DP_URL.format(asin=asin)
    last_err = ""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            client.headers["User-Agent"] = random.choice(USER_AGENTS)
            resp = client.get(url)
            code = resp.status_code
            if code == 404:
                return ScrapeResult(asin, "failed", attempts=attempt,
                                    error="404 商品页不存在/已下架")
            if code != 200:
                last_err = f"HTTP {code}"
            elif "validateCaptcha" in str(resp.url) or CAPTCHA_RE.search(resp.text):
                last_err = "命中验证码/反爬页面"
            else:
                rating, reviews = parse_product_page(resp.text)
                if rating and reviews:
                    return ScrapeResult(asin, "ok", rating, reviews,
                                        attempts=attempt)
                if rating is None and reviews is None \
                        and PRODUCT_TITLE_RE.search(resp.text):
                    return ScrapeResult(asin, "no_rating", attempts=attempt)
                last_err = "页面解析不完整（缺评分或评论数元素）"
        except Exception as e:                      # 超时/连接错误等
            last_err = f"{type(e).__name__}: {e}"
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_BACKOFF[attempt - 1] + random.uniform(0, 2))
    return ScrapeResult(asin, "failed", attempts=MAX_RETRIES, error=last_err)

# ============================== 主流程 ==============================


def main():
    global _ACTIVE_URL
    dry_run = "--dry-run" in sys.argv
    limit = None
    if "--limit" in sys.argv:
        i = sys.argv.index("--limit")
        limit = int(sys.argv[i + 1])
    if "--url" in sys.argv:
        i = sys.argv.index("--url")
        _ACTIVE_URL = sys.argv[i + 1]
        print(f"使用指定表格: {_ACTIVE_URL}", flush=True)

    if shutil.which("lark-cli") is None:
        raise FatalError("未找到 lark-cli，请确认已安装并在 PATH 中")

    started = time.time()
    print(f"[{TODAY}] 亚马逊商品监控开始{'（dry-run，不写回）' if dry_run else ''}",
          flush=True)

    # 1. 读取清单
    verify_sheets_exist()
    infos = [analyze_sheet(n) for n in SHEET_NAMES]
    for info in infos:
        col_desc = (f"已存在今日列 {col_letter(info.today_col_idx)}"
                    if info.today_col_idx is not None else "无今日列，将插入 B 列")
        print(f"工作表「{info.name}」: {len(info.asin_rows)} 个 ASIN，{col_desc}",
              flush=True)

    # 2. 跨表去重，保持首次出现顺序
    asins = []
    for info in infos:
        for asin in info.asin_rows:
            if asin not in asins:
                asins.append(asin)
    if limit:
        asins = asins[:limit]
    print(f"去重后待抓取: {len(asins)} 个", flush=True)

    # 3. 抓取
    results = {}
    client = make_client()
    for i, asin in enumerate(asins):
        res = fetch_product(client, asin)
        results[asin] = res
        if res.status == "ok":
            detail = res.cell_text()
        elif res.status == "no_rating":
            detail = "无评分"
        else:
            detail = f"失败（{res.error}）"
        print(f"  [{i + 1}/{len(asins)}] {asin} -> {detail}"
              f"（第 {res.attempts} 次请求）", flush=True)
        if i < len(asins) - 1:
            time.sleep(random.uniform(REQUEST_INTERVAL_MIN, REQUEST_INTERVAL_MAX))
    client.close()

    # 4. 写回表格
    for info in infos:
        col, inserted = ensure_today_column(info, dry_run)
        n = write_results(info, col, results, dry_run)
        action = "将写入" if dry_run else "已写入"
        col_desc = f"{col} 列（新插入）" if inserted else f"{col} 列"
        print(f"工作表「{info.name}」: {action} {n} 条 -> {col_desc}", flush=True)

    # 5. 对比前一天，不一致把当天单元格标黄
    for info in infos:
        _n, desc = compare_and_highlight(info.name, dry_run)
        print(f"工作表「{info.name}」: {desc}", flush=True)

    # 6. 摘要
    ok = sum(1 for r in results.values() if r.status == "ok")
    nr = sum(1 for r in results.values() if r.status == "no_rating")
    failed = {a: r.error for a, r in results.items() if r.status == "failed"}
    elapsed = int(time.time() - started)
    print("================ 运行摘要 ================", flush=True)
    print(f"日期: {TODAY}", flush=True)
    print(f"抓取: {len(results)} | 成功: {ok} | 无评分: {nr} | 失败: {len(failed)}",
          flush=True)
    if failed:
        print("失败明细:", flush=True)
        for asin, err in failed.items():
            print(f"  - {asin}: {err}", flush=True)
    print(f"耗时: {elapsed}s", flush=True)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except FatalError as e:
        print(f"致命错误: {e}", file=sys.stderr)
        sys.exit(2)
