#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
amazon_monitor.py — 亚马逊商品评分/评论数每日监控

每天定时由 TRAE 定时任务触发，完整流程：
  1. 读取飞书表格全部工作表 A 列的 ASIN（行号映射）
  2. 抓取亚马逊美国站商品页 https://www.amazon.com/dp/{ASIN}
     - 相邻请求随机间隔 1.3~3.0 秒，UA 池轮换，Session 保持 Cookie
     - 失败重试最多 3 次（退避 5/10/15 秒 + 随机抖动）
     - 拦截页（验证码/软拦截/无 productTitle）快速失败，不做 3 次空转重试
     - 连续 6 个页面被拦截即熔断中止，避免整列写入“抓取失败”污染数据
  3. 结果写回同一表格：每个商品一行、每天一列（最新日期插在 B 列，历史保留）
     单元格格式沿用现有约定："4.8 (97)"；无评分写 "-"；最终失败写 "抓取失败"
     每抓 20 个 ASIN 分批写回一次，前台超时被 kill 也不会丢失已抓数据
  4. 数据写回后，把当天与前一天对比，值不一致的当天单元格标黄（#FFFF00）
  5. stdout 输出运行摘要，退出码 0=正常结束（允许部分商品失败），2=致命错误，
     3=疑似反爬熔断中止（未写入的 ASIN 保持空列，换 IP 后重跑可覆盖补齐）

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

try:
    import httpx
except ModuleNotFoundError:
    # 定时任务所在环境可能被重置而缺依赖：自动补装后重新导入，保证任务可用
    print("未找到 httpx，正在自动安装...", file=sys.stderr, flush=True)
    subprocess.run([sys.executable, "-m", "pip", "install", "--quiet", "httpx"],
                   check=True)
    import httpx

# ============================== 配置区 ==============================

SPREADSHEET_URL = "https://tcn4m7idpero.feishu.cn/sheets/HpK9swqZwhiCKztODP3c17twn4f"

# 当前实际使用的表格 URL；默认生产表，也可用 --url 覆盖（用于测试副本表）
_ACTIVE_URL = SPREADSHEET_URL

AMAZON_DP_URL = "https://www.amazon.com/dp/{asin}"

REQUEST_INTERVAL_MIN = 1.3      # 相邻商品请求最小间隔（秒）
REQUEST_INTERVAL_MAX = 3.0      # 相邻商品请求最大间隔（秒）
UA_VERIFY_INTERVAL_MIN = 6.0    # 空页面疑似反爬时，换 UA 重试前的更慢间隔（降低频率）
UA_VERIFY_INTERVAL_MAX = 10.0
MAX_RETRIES = 3                 # 单链接最大尝试次数
RETRY_BACKOFF = [5, 10, 15]     # 第 1/2/3 次重试前的退避秒数（另加 0~2 秒抖动）
REQUEST_TIMEOUT = 25            # 单次 HTTP 超时（秒）
SUMMARY_WINDOW = 20000          # 主评分摘要区解析窗口（#averageCustomerReviews 之后字符数）

BLOCK_ABORT_THRESHOLD = 6       # 连续 N 个页面被拦截即判定为整体反爬，立即中止任务
MIN_PAGE_LEN = 5000             # 页面 HTML 过短视为异常页（正常商品页远大于此）

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
    # 旧版：acrCustomerReviewText 文本 "(83)"
    re.compile(r'id="acrCustomerReviewText"[^>]*>\s*\(?([\d,]+)\)?\s*<'),
    # 旧版：acrCustomerReviewText 的 aria-label="83 Reviews"
    re.compile(r'id="acrCustomerReviewText"[^>]*aria-label="([\d,]+)', re.I),
    # 旧版：通用 aria-label="N Reviews/Ratings"
    re.compile(r'aria-label="([\d,]+)\s+(?:Reviews?|ratings?)"', re.I),
    # 新版：aria-label/文本 "4.2 out of 5 stars, 216 ratings"（主区限定）
    re.compile(r'out of 5 stars[,:\s]*([\d,]+)\s+ratings?', re.I),
    # 新版：文本 "N global ratings"
    re.compile(r'([\d,]+)\s+global ratings', re.I),
]
# 新版页面懒加载标记：评分摘要由前端 AJAX 渲染，服务端 HTML 无评分/评论数
LAZY_REVIEWS_RE = re.compile(r'lazyWidgetLoaderUrl|data-lazy-widget-triggered="false"')
# 新版页面直方图百分比（判断商品是否真的有评分）
HISTOGRAM_PCT_RE = re.compile(r'([\d.]+)\s*percent of reviews have', re.I)
CAPTCHA_RE = re.compile(
    r'(Enter the characters you see below|Type the characters you see in this image'
    r'|Robot Check|validateCaptcha|are not a robot)', re.I)
# 亚马逊软拦截/异常页特征（HTTP 200 但无 productTitle 的常见封锁页）
BLOCK_RE = re.compile(
    r'(Sorry! Something went wrong'
    r'|api-services-support@amazon\.com'
    r'|To discuss automated access to Amazon data'
    r'|automated access to our (?:site|data)'
    r'|Request could not be satisfied'
    r'|Bot Verification'
    r'|we just need to make sure you.re not a robot'
    r'|Service Unavailable)', re.I)
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
    status: str                          # "ok" | "no_rating" | "blocked" | "failed"
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


def write_results(info, col, results, dry_run, only_success=False):
    """把抓取结果批量写入指定列。返回写入条数。

    only_success=True 时只写 ok/no_rating，跳过 blocked/failed
    （用于反爬熔断时避免把整列写成“抓取失败”污染数据）。
    """
    writes = []
    for asin, rn in info.asin_rows.items():
        res = results.get(asin)
        if res is None:
            continue
        if only_success and res.status not in ("ok", "no_rating"):
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


def list_sheet_names():
    """列出工作簿中全部工作表名（读取所有工作表 A 列 ASIN）。"""
    data = lark(["+workbook-info"])
    names = [s["sheet_name"] for s in data.get("sheets", [])]
    if not names:
        raise FatalError("工作簿中没有工作表")
    return names

# ============================== 亚马逊抓取 ==============================


def parse_product_page(html):
    """从商品页 HTML 提取 (评分, 评论数, 懒加载标记, 是否有评分直方图)。

    只在主商品评分摘要区（#averageCustomerReviews / #productTitle 附近的
    SUMMARY_WINDOW 窗口内）解析，避免把页面后方“相关商品/推荐位”卡片的
    评分、评论数误当作主商品数据。
    """
    m = PRODUCT_TITLE_RE.search(html)
    title_pos = m.start() if m else None
    avg = re.search(r'id="averageCustomerReviews"', html)
    anchor = avg.start() if avg else title_pos

    if anchor is not None:
        window = html[anchor:anchor + SUMMARY_WINDOW]
    else:
        window = html  # 无标题/摘要锚点时退化为全页（极少见）

    rating = None
    m = RATING_RE.search(window)
    if not m:
        m = RATING_RE_FALLBACK.search(window)
    if m:
        rating = m.group(1)

    reviews = None
    for rx in REVIEWS_RES:
        mr = rx.search(window)
        if mr:
            reviews = mr.group(1).replace(",", "")
            break

    # 新版页面懒加载：评分摘要由前端 AJAX 渲染，服务端 HTML 无评分/评论数
    lazy = LAZY_REVIEWS_RE.search(html) is not None
    # 直方图百分比：全部为 0% 说明商品确实没有评分（有评分则存在非零百分比）
    hist_pcts = [float(x) for x in HISTOGRAM_PCT_RE.findall(html)]
    has_ratings = any(p > 0 for p in hist_pcts) if hist_pcts else False
    return rating, reviews, lazy, has_ratings


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
    """抓取单个 ASIN，含重试。返回 ScrapeResult。

    遇到"空页面"（懒加载空数据）时，可能是反爬导致部分 JS 未加载、服务端
    无评分数据。此时会放慢间隔并更换 User-Agent 重新抓一次以验证：换 UA 后
    能取到数据 → 判定为反爬，本次按成功返回并标记已降低频率；换 UA 后仍取
    不到 → 确认为商品本身无评分/懒加载空数据。
    """
    url = AMAZON_DP_URL.format(asin=asin)

    def _get():
        client.headers["User-Agent"] = random.choice(USER_AGENTS)
        return client.get(url)

    def _verify_ua(attempt):
        """空页面验证：放慢频率 + 换 UA 再抓一次。成功返回 ScrapeResult，否则 None。"""
        time.sleep(random.uniform(UA_VERIFY_INTERVAL_MIN, UA_VERIFY_INTERVAL_MAX))
        client.headers["User-Agent"] = random.choice(USER_AGENTS)
        try:
            resp = client.get(url)
        except Exception:
            return None
        if resp.status_code != 200 or CAPTCHA_RE.search(resp.text):
            return None
        rating, reviews, lazy, has_ratings = parse_product_page(resp.text)
        if rating and reviews and reviews != "0":
            return ScrapeResult(asin, "ok", rating, reviews, attempts=attempt,
                                error="疑似反爬：换 User-Agent 后成功（已降低频率）")
        return None

    last_err = ""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = _get()
            code = resp.status_code
            if code == 404:
                return ScrapeResult(asin, "failed", attempts=attempt,
                                    error="404 商品页不存在/已下架")
            if code != 200:
                last_err = f"HTTP {code}"
            elif "validateCaptcha" in str(resp.url) or CAPTCHA_RE.search(resp.text):
                # 验证码页：立即判为拦截，不再重试 3 次白耗 30s/个
                return ScrapeResult(asin, "blocked", attempts=attempt,
                                    error="命中验证码页")
            elif BLOCK_RE.search(resp.text) or len(resp.text) < MIN_PAGE_LEN:
                # 软拦截页/异常短页：立即判为拦截
                return ScrapeResult(asin, "blocked", attempts=attempt,
                                    error="疑似反爬拦截（软拦截页/页面过短）")
            else:
                rating, reviews, lazy, has_ratings = parse_product_page(resp.text)
                if rating and reviews and reviews != "0":
                    return ScrapeResult(asin, "ok", rating, reviews,
                                        attempts=attempt)
                if PRODUCT_TITLE_RE.search(resp.text):
                    if rating is None or reviews is None or reviews == "0":
                        if has_ratings:
                            # 直方图显示有评分但解析不到：可能反爬，换 UA 验证
                            vr = _verify_ua(attempt)
                            if vr is not None:
                                return vr
                            return ScrapeResult(asin, "no_rating", attempts=attempt,
                                                error="懒加载/疑似反爬，换 UA 后仍无数据")
                        # 直方图缺失或全为 0%：商品确实没有评分/评论，直接判无评分
                        # （不再为此类页面做换 UA 验证，避免每个空页面多耗 6-10s）
                        return ScrapeResult(asin, "no_rating", attempts=attempt)
                    last_err = "页面解析不完整（缺评分或评论数元素）"
                else:
                    # 无 productTitle：判定为拦截页并立即返回（不重试），
                    # 由主流程的连续拦截熔断机制整体中止，避免空转 60+ 分钟
                    return ScrapeResult(asin, "blocked", attempts=attempt,
                                        error="未找到商品标题（productTitle），疑似反爬拦截")
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

    # 1. 读取清单（动态获取全部工作表，读取每张表 A 列 ASIN）
    sheet_names = list_sheet_names()
    print(f"工作表: {sheet_names}", flush=True)
    infos = [analyze_sheet(n) for n in sheet_names]
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

    # 3. 先建好所有工作表的今日列（供分批写回使用，避免重复插入）
    today_cols = {}
    for info in infos:
        col, inserted = ensure_today_column(info, dry_run)
        today_cols[info.name] = col
        if not dry_run:
            col_desc = f"{col} 列（新插入）" if inserted else f"{col} 列"
            print(f"工作表「{info.name}」: 今日列 {col_desc} 已就绪", flush=True)

    # 4. 分批抓取 + 分批写回：每 BATCH_SIZE 个写回一次，防止前台超时被 kill
    #    导致全部结果丢失（此前 99/267 被 kill、未触发写回的问题）
    BATCH_SIZE = 20
    results = {}
    batch = {}
    consecutive_blocked = 0
    blocked_systemic = False
    client = make_client()

    def flush(batch_results, only_success=False):
        """把一批结果写回所有工作表；批次为空则跳过。"""
        if not batch_results:
            return
        for info in infos:
            write_results(info, today_cols[info.name], batch_results,
                          dry_run, only_success=only_success)

    try:
        for i, asin in enumerate(asins):
            res = fetch_product(client, asin)
            results[asin] = res
            batch[asin] = res
            if res.status == "blocked":
                consecutive_blocked += 1
            else:
                consecutive_blocked = 0

            if res.status == "ok":
                detail = res.cell_text()
            elif res.status == "no_rating":
                detail = "无评分"
            elif res.status == "blocked":
                detail = f"被拦截（{res.error}）"
            else:
                detail = f"失败（{res.error}）"
            print(f"  [{i + 1}/{len(asins)}] {asin} -> {detail}"
                  f"（第 {res.attempts} 次请求）", flush=True)

            # 熔断：连续多个页面被拦截，判定整体反爬，立即中止（不写失败值）
            if consecutive_blocked >= BLOCK_ABORT_THRESHOLD:
                blocked_systemic = True
                flush(batch, only_success=True)
                batch = {}
                print(f"\n[熔断] 连续 {consecutive_blocked} 个页面被反爬拦截，"
                      f"判定当前出口 IP 被亚马逊封锁，立即中止任务。", flush=True)
                print(f"        已处理 {i + 1}/{len(asins)} 个，"
                      f"其余 ASIN 不写入（保持空列，避免写入“{FAIL_TEXT}”污染数据）。",
                      flush=True)
                print("        处理建议：更换出口 IP/代理后重跑；"
                      "今日列已存在，重跑为覆盖写入，可自动补齐。", flush=True)
                break

            if (i + 1) % BATCH_SIZE == 0 or i == len(asins) - 1:
                action = "将写回" if dry_run else "已写回"
                flush(batch)
                print(f"  [批次写回] 进度 {i + 1}/{len(asins)}，"
                      f"本批 {len(batch)} 个{action}", flush=True)
                batch = {}
            if i < len(asins) - 1:
                time.sleep(random.uniform(REQUEST_INTERVAL_MIN, REQUEST_INTERVAL_MAX))
    finally:
        # 无论正常结束、熔断中止还是异常退出，都确保最后一个不足 20 的批次写回
        flush(batch)
        client.close()

    # 4.1 熔断中止：输出诊断摘要并按退出码 3 结束（跳过标黄对比）
    if blocked_systemic:
        ok = sum(1 for r in results.values() if r.status == "ok")
        nr = sum(1 for r in results.values() if r.status == "no_rating")
        blk = sum(1 for r in results.values() if r.status == "blocked")
        print("============ 运行中止（疑似反爬拦截） ============", flush=True)
        print(f"日期: {TODAY}", flush=True)
        print(f"已抓取: {len(results)} | 成功: {ok} | 无评分: {nr} | 被拦截: {blk}",
              flush=True)
        print(f"耗时: {int(time.time() - started)}s", flush=True)
        print("说明: 被拦截/未处理的 ASIN 未写入，保持空列。换 IP 后重跑即可覆盖补齐。",
              flush=True)
        return 3

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
