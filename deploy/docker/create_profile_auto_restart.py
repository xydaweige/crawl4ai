#!/usr/bin/env python3
"""
简单的 Profile 管理工具 - 使用官方 BrowserProfiler
（自动重启服务版本）

用法:
  python create_profile_auto_restart.py create <name>    # 创建
  python create_profile_auto_restart.py edit website      # 编辑
  python create_profile_auto_restart.py list             # 列出
"""

import asyncio
import sys
import os
import glob
import subprocess
from crawl4ai import BrowserProfiler
from playwright.async_api import async_playwright

os.environ["DISPLAY"] = ":1"


def cleanup(profile_path):
    """清理锁文件和进程"""
    # 清理锁文件
    for lock in glob.glob(f"{profile_path}/**/SingletonLock", recursive=True):
        try:
            os.remove(lock)
        except:
            pass

    for lock in glob.glob(f"{profile_path}/**/SingletonSocket", recursive=True):
        try:
            os.remove(lock)
        except:
            pass

    # 终止进程
    try:
        import subprocess

        subprocess.run(["pkill", "-9", "-f", profile_path], capture_output=True)
    except:
        pass


async def create(name):
    """创建 profile"""
    profiler = BrowserProfiler()
    print(f"创建 profile: {name}")
    print("浏览器启动后，在 VNC 中登录，然后按 'q' 保存")

    # 先检查是否已存在该profile，如果存在则先清理锁文件
    profiles = profiler.list_profiles()
    for p in profiles:
        if p["name"] == name:
            print(f"🧹 发现已存在的 profile，先清理锁文件...")
            cleanup(p["path"])
            break

    path = await profiler.create_profile(name)
    print(f"✅ 已创建: {path}")


async def edit(name):
    """编辑 profile"""
    profiler = BrowserProfiler()
    profiles = profiler.list_profiles()

    # 查找 profile
    path = None
    for p in profiles:
        if p["name"] == name:
            path = p["path"]
            break

    if not path:
        print(f"❌ Profile '{name}' 不存在")
        return

    print(f"编辑 profile: {name}")
    print("浏览器启动后，按 'q' 保存")

    cleanup(path)  # 启动前清理

    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            user_data_dir=path,
            headless=False,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )

        print("✅ 浏览器已启动，按 'q' 保存...")

        # 等待 'q' 键
        if sys.stdin.isatty():
            import tty, termios

            old = termios.tcgetattr(sys.stdin)
            try:
                tty.setcbreak(sys.stdin.fileno())
                while sys.stdin.read(1) != "q":
                    pass
            finally:
                termios.tcsetattr(sys.stdin, termios.TCSADRAIN, old)
        else:
            input("\n按 Enter 保存...")

    cleanup(path)  # 关闭后清理
    print(f"✅ 已更新: {path}")


async def list_profiles():
    """列出所有 profiles"""
    profiler = BrowserProfiler()
    profiles = profiler.list_profiles()

    if not profiles:
        print("📭 还没有 profiles")
    else:
        print(f"📋 共 {len(profiles)} 个 profile:")
        for p in profiles:
            print(f"  - {p['name']}: {p['path']}")


async def main():
    if len(sys.argv) < 2:
        print("用法:")
        print("  python create_profile_auto_restart.py create <name>")
        print("  python create_profile_auto_restart.py edit <name>")
        print("  python create_profile_auto_restart.py list")
        return

    cmd = sys.argv[1].lower()

    if cmd == "list":
        await list_profiles()
    elif cmd == "create":
        if len(sys.argv) < 3:
            print("❌ 请提供 profile 名称")
            return
        await create(sys.argv[2])
    elif cmd == "edit":
        if len(sys.argv) < 3:
            print("❌ 请提供 profile 名称")
            return
        await edit(sys.argv[2])
    else:
        print("❌ 未知命令")
        return

    # 操作完成后重启 gunicorn 服务（容器内部）
    print("\n" + "=" * 60)
    print("🔄 正在重启 gunicorn 服务...")
    print("=" * 60)

    try:
        # 使用 supervisorctl 重启 gunicorn
        print("执行命令: supervisorctl restart gunicorn")
        result = subprocess.run(
            ["supervisorctl", "restart", "gunicorn"],
            capture_output=True,
            text=True,
            timeout=30,
        )

        print(f"返回码: {result.returncode}")
        if result.stdout:
            print(f"标准输出:\n{result.stdout}")
        if result.stderr:
            print(f"标准错误:\n{result.stderr}")

        if result.returncode == 0:
            print("\n✅ Gunicorn 服务已重启！")
            print("\n💡 说明: 只重启了 gunicorn 服务，容器未重启")
            print("   如果需要完全重启容器，请在宿主机运行:")
            print("   docker-compose -f docker-compose-gui.yml restart")
        else:
            print(f"\n❌ 服务重启失败 (返回码: {result.returncode})")
    except subprocess.TimeoutExpired:
        print("❌ 服务重启超时")
    except FileNotFoundError as e:
        print(f"⚠️  supervisorctl 未找到: {e}")
        print("   这可能意味着脚本不在容器内运行")
        print("   请手动重启容器或在容器内运行此脚本")
    except Exception as e:
        print(f"❌ 重启出错: {str(e)}")
        import traceback

        traceback.print_exc()

    print("\n" + "=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
