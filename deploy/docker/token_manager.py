"""
Token 管理模块
负责加载和管理各网站的认证 Token
"""

import json
import os
import logging
from typing import Optional, Dict
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

TOKEN_DIR = "/app/auth_tokens"


def load_token_for_domain(domain: str) -> Optional[Dict]:
    """
    加载指定域的 token 配置
    
    Args:
        domain: 网站域名（如 work.hisign.com.cn）
    
    Returns:
        Token 配置字典，如果未找到返回 None
    """
    # 1. 尝试精确匹配
    token_file = os.path.join(TOKEN_DIR, f"{domain.replace('.', '_')}.json")

    if os.path.exists(token_file):
        try:
            with open(token_file, 'r') as f:
                config = json.load(f)

            if config.get('enabled', True):
                logger.info(f"Loaded token config for domain: {domain}")
                return config
            else:
                logger.info(f"Token config disabled for domain: {domain}")
                return None
        except Exception as e:
            logger.error(f"Error loading token file {token_file}: {e}")
            return None

    # 2. 尝试模糊匹配（子域名匹配）
    if os.path.exists(TOKEN_DIR):
        for filename in os.listdir(TOKEN_DIR):
            if filename.endswith('.json'):
                try:
                    config_path = os.path.join(TOKEN_DIR, filename)
                    with open(config_path, 'r') as f:
                        config = json.load(f)

                    # 检查域名是否匹配
                    config_domain = config.get('domain', '')
                    if domain == config_domain or domain.endswith(config_domain):
                        if config.get('enabled', True):
                            logger.info(f"Loaded token config for domain: {domain} (matched: {config_domain})")
                            return config
                except Exception as e:
                    logger.warning(f"Error reading token file {filename}: {e}")
                    continue

    logger.info(f"No token config found for domain: {domain}")
    return None


def extract_domain_from_url(url: str) -> str:
    """
    从 URL 中提取域名
    
    Args:
        url: 网站 URL
    
    Returns:
        域名
    """
    try:
        parsed = urlparse(url)
        return parsed.netloc
    except Exception as e:
        logger.warning(f"Failed to parse URL {url}: {e}")
        return ""


def generate_hooks_code(token_config: Dict) -> Dict[str, str]:
    """
    根据 Token 配置生成 Hooks 代码
    
    Args:
        token_config: Token 配置字典
    
    Returns:
        Hooks 代码字典
    """
    token = token_config['token']
    domain = token_config['domain']
    cookie_name = token_config.get('cookie_name', 'token')
    header_format = token_config.get('header_format', 'Bearer {token}')

    # 替换 header_format 中的 {token}
    header_value = header_format.replace('{token}', token)

    # 检查是否有自定义 Hooks
    custom_hooks = token_config.get('hooks', {})

    hooks_code = {}

    # on_page_context_created Hook
    if custom_hooks.get('on_page_context_created'):
        hooks_code['on_page_context_created'] = custom_hooks['on_page_context_created']
    else:
        hooks_code['on_page_context_created'] = f'''async def hook(page, context, **kwargs):
    await context.add_cookies([{{"name": "{cookie_name}", "value": "{token}", "domain": "{domain}", "path": "/"}}])
    return page'''

    # before_goto Hook
    if custom_hooks.get('before_goto'):
        hooks_code['before_goto'] = custom_hooks['before_goto']
    else:
        hooks_code['before_goto'] = f'''async def hook(page, context, url, **kwargs):
    await page.set_extra_http_headers({{"authorization": "{header_value}"}})
    return page'''

    return hooks_code


def get_crawler_config_overrides(token_config: Dict) -> Dict:
    """
    获取额外的 crawler 配置
    
    Args:
        token_config: Token 配置字典
    
    Returns:
        额外的配置字典
    """
    return token_config.get('crawler_config', {})