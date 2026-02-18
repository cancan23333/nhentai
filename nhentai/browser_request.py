# browser_request.py
import time
import os
from requests_html import HTMLSession
from nhentai import constant
from nhentai.logger import logger

class BrowserRequest:
    def __init__(self):
        self.session = HTMLSession()
        self.setup_headers()
        self.setup_proxy()
        
    def setup_headers(self):
        """设置完整的浏览器headers"""
        headers = {
            'authority': 'nhentai.net',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'max-age=0',
            'dnt': '1',
            'referer': 'https://nhentai.net/',
            'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        }
        
        # 添加cookie
        cookie = constant.CONFIG.get('cookie')
        if cookie and cookie.strip():
            headers['Cookie'] = cookie
            
        self.session.headers.update(headers)
        
    def setup_proxy(self):
        """设置代理"""
        proxy = constant.CONFIG.get('proxy')
        if proxy and proxy.strip():
            proxies = {
                'http': proxy,
                'https': proxy,
            }
            self.session.proxies.update(proxies)
    
    def get(self, url, retry_count=0, max_retries=3):
        """发送GET请求，自动处理JavaScript渲染"""
        try:
            logger.debug(f'BrowserRequest: Sending GET to {url}')
            
            # 使用requests-html渲染JavaScript
            response = self.session.get(url)
            
            # 如果需要JavaScript执行
            if '<script>' in response.text or 'Cloudflare' in response.text:
                response.html.render(timeout=20)
                time.sleep(2)  # 等待渲染完成
            
            logger.debug(f'BrowserRequest: Status {response.status_code}')
            
            # 检查是否仍有CloudFlare挑战
            if response.status_code == 403 or 'cf-mitigated' in response.headers:
                if retry_count < max_retries:
                    logger.warning(f'CloudFlare detected, retrying ({retry_count+1}/{max_retries})...')
                    time.sleep(5 * (retry_count + 1))
                    return self.get(url, retry_count + 1, max_retries)
                else:
                    logger.error('Failed to bypass CloudFlare after multiple attempts')
                    return None
            
            return response
            
        except Exception as e:
            logger.error(f'BrowserRequest error: {e}')
            if retry_count < max_retries:
                time.sleep(3)
                return self.get(url, retry_count + 1, max_retries)
            return None


# 单例实例
_browser_request = None

def get_browser_request():
    global _browser_request
    if _browser_request is None:
        _browser_request = BrowserRequest()
    return _browser_request
