#!/usr/bin/env python3
"""
获取 hisign 网站的认证 token

用法:
  python get_hisign_token.py

功能:
  1. 打开浏览器访问 hisign 网站
  2. 等待你手动登录
  3. 登录成功后提取 md_pss_id token
  4. 保存到 hisign_config.json
"""

import asyncio
import json
from datetime import datetime
from playwright.async_api import async_playwright
import os

os.environ["DISPLAY"] = ":1"


async def get_token():
    print("=" * 60)
    print("🔑 获取 Hisign 认证 Token")
    print("=" * 60)

    async with async_playwright() as p:
        # 使用临时 profile
        browser = await p.chromium.launch_persistent_context(
            user_data_dir="/home/appuser/.crawl4ai/profiles/get-token",
            headless=False,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )

        page = browser.pages[0] if browser.pages else await browser.new_page()

        print("\n🚀 正在打开浏览器...")
        await page.goto("https://work.hisign.com.cn/", wait_until="networkidle")

        print("\n" + "=" * 60)
        print("📝 请在浏览器（VNC）中完成登录：")
        print("   1. 输入手机号/密码")
        print("   2. 完成登录")
        print("   3. 确认能看到登录后的页面")
        print("=" * 60)
        print("\n⏳ 登录成功后，按 Enter 继续...")
        input()

        # 等待一下，确保登录完成
        await asyncio.sleep(2)

        # 刷新页面，确保 token 生效
        await page.reload(wait_until="networkidle")
        await asyncio.sleep(2)

        # 提取 cookies
        cookies = await browser.cookies()

        # 查找 md_pss_id
        token = None
        for cookie in cookies:
            if cookie["name"] == "md_pss_id" and "hisign" in cookie.get("domain", ""):
                token = cookie["value"]
                break

        print("\n" + "=" * 60)
        if token:
            print(f"✅ 成功找到 Token!")
            print(f"\n🔑 Token: {token}")

            # 保存到配置文件
            config = {
                "token": token,
                "updated": datetime.now().isoformat(),
                "url": "https://work.hisign.com.cn/",
            }

            config_file = "/home/appuser/hisign_config.json"
            with open(config_file, "w", encoding="utf-8") as f:
                json.dump(config, f, indent=2, ensure_ascii=False)

            print(f"✅ 已保存到: {config_file}")

            # 验证页面
            content = await page.content()
            if "Login" not in content:
                print("\n✅ 验证成功：页面已登录")
            else:
                print("\n⚠️  警告：页面仍显示登录界面")
        else:
            print("❌ 未找到 md_pss_id token")
            print("\n🍪 所有 Cookies:")
            for cookie in cookies:
                if "hisign" in cookie.get("domain", ""):
                    print(f"  - {cookie['name']}: {cookie['value'][:50]}")

        print("=" * 60)

        if token:
            print("\n✅ 完成！现在可以使用 crawl_hisign.py 进行爬取了")
        else:
            print("\n❌ 获取失败，请检查是否登录成功")

        print("\n按 Enter 关闭浏览器...")
        input()

        await browser.close()


if __name__ == "__main__":
    try:
        asyncio.run(get_token())
    except KeyboardInterrupt:
        print("\n\n⚠️  操作已取消")
    except Exception as e:
        print(f"\n❌ 错误: {e}")
        import traceback

        traceback.print_exc()
