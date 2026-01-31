#!/usr/bin/env python3
"""
使用保存的 token 爬取 hisign 网站

用法:
  python crawl_hisign.py

前置条件:
  1. 先运行 get_hisign_token.py 获取 token
  2. token 会保存在 hisign_config.json
"""

import asyncio
import json
import os
import sys
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig

os.environ["DISPLAY"] = ":1"

CONFIG_FILE = "/home/appuser/hisign_config.json"


def load_token():
    """加载 token"""
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            config = json.load(f)
        token = config.get("token")
        if not token:
            print(f"❌ 配置文件中没有 token")
            return None
        return token
    except FileNotFoundError:
        print(f"❌ 配置文件不存在: {CONFIG_FILE}")
        print(f"请先运行: python get_hisign_token.py")
        return None
    except Exception as e:
        print(f"❌ 加载配置文件失败: {e}")
        return None


async def on_page_context_created(page, context, **kwargs):
    """设置 Cookie"""
    await context.add_cookies(
        [
            {
                "name": "md_pss_id",
                "value": TOKEN,
                "domain": "work.hisign.com.cn",
                "path": "/",
            }
        ]
    )
    return page


async def before_goto(page, context, url, **kwargs):
    """设置请求头"""
    await page.set_extra_http_headers({"authorization": f"md_pss_id {TOKEN}"})
    return page


async def crawl_hisign(url="https://work.hisign.com.cn/"):
    """爬取 hisign 网站"""
    print("=" * 60)
    print("🔍 爬取 Hisign 网站")
    print("=" * 60)
    print(f"\n📌 URL: {url}")
    print(f"🔑 Token: {TOKEN[:50]}...")
    print("=" * 60)

    browser_config = BrowserConfig(headless=True, verbose=True)

    crawler = AsyncWebCrawler(config=browser_config)

    # 设置 Hooks
    crawler.crawler_strategy.set_hook(
        "on_page_context_created", on_page_context_created
    )
    crawler.crawler_strategy.set_hook("before_goto", before_goto)

    await crawler.start()

    print("\n🚀 开始爬取...")
    result = await crawler.arun(
        url=url,
        config=CrawlerRunConfig(
            cache_mode="bypass",
            js_code="window.scrollTo(0, document.body.scrollHeight);",
            wait_for="body",
            page_timeout=30000,
            delay_before_return_html=3.0,
        ),
    )

    if result.success:
        print("\n✅ 爬取成功!")
        print(f"📄 内容长度: {len(result.markdown)} 字符")
        print(f"🔗 URL: {result.url}")

        # 检查是否登录成功
        if "Login" in result.markdown or "登录" in result.markdown:
            print("\n⚠️  内容包含'登录'或'Login'，Token 可能已过期")
            print("请重新运行: python get_hisign_token.py")
        else:
            print("\n✅ 登录状态正常！")

        # 显示前 500 字符
        print("\n📝 内容预览（前 500 字符）:")
        print("-" * 60)
        print(result.markdown[:500])
        print("-" * 60)

        # 保存完整结果
        output_file = "hisign_result.md"
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(result.markdown)
        print(f"\n✅ 完整结果已保存到: {output_file}")

    else:
        print(f"\n❌ 爬取失败: {result.error_message}")

    await crawler.close()
    print("=" * 60)


if __name__ == "__main__":
    # 加载 token
    TOKEN = load_token()
    if not TOKEN:
        sys.exit(1)

    print(f"\n✅ 成功加载 Token")

    # 爬取
    try:
        asyncio.run(crawl_hisign())
    except KeyboardInterrupt:
        print("\n\n⚠️  操作已取消")
    except Exception as e:
        print(f"\n❌ 错误: {e}")
        import traceback

        traceback.print_exc()
