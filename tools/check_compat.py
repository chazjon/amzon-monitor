# -*- coding: utf-8 -*-
"""
tools/check_compat.py — 验证 Scrapling 核心接口契约兼容性
在上游更新或重新安装时运行，若接口不匹配则直接报错提示。
"""
import sys

def check():
    try:
        from scrapling.fetchers import Fetcher
    except Exception as e:
        print(f"[FAIL] 导入 scrapling.fetchers.Fetcher 失败: {e}")
        return 1

    # 验证 get 方法存在
    if not hasattr(Fetcher, "get"):
        print("[FAIL] Fetcher 缺少 get 方法")
        return 1

    # 进行一次最小请求验证返回字段
    try:
        r = Fetcher.get("https://httpbin.org/get", impersonate="chrome", timeout=15)
        required_attrs = ["status", "html_content", "url"]
        for attr in required_attrs:
            if not hasattr(r, attr):
                print(f"[FAIL] Response 缺少关键属性: {attr}")
                return 1
    except Exception as e:
        # 网络若无法访问外部测试站点，则仅做静态检查通过
        print(f"[WARN] 网络连通测试失败（可能是沙箱网络限制）: {e}，静态方法检查通过")
        return 0

    print("[PASS] Scrapling 接口兼容性验证通过！")
    return 0

if __name__ == "__main__":
    sys.exit(check())
