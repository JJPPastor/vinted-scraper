from playwright.sync_api import sync_playwright
import json
import time
import os
import random
import requests
from urllib.parse import urlencode
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

VINTED_URL = "https://www.vinted.fr/"
API_TEST = "https://www.vinted.fr/api/v2/catalog/items?page=1&per_page=10"

BRIGHT_PROXY = os.getenv('VINTED_PROXY_HTTPS') or os.getenv('VINTED_PROXY_HTTP')


def launch_and_get_session(proxy=None, headless=True):
    with sync_playwright() as p:
        proxy_conf = None
        if proxy:
            raw = proxy
            if raw.startswith('http://'):
                raw = raw[len('http://'):]
            user = None
            password = None
            if '@' in raw:
                creds, host = raw.split('@', 1)
                if ':' in creds:
                    user, password = creds.split(':', 1)
                proxy_server = 'http://' + host
            else:
                proxy_server = 'http://' + raw
            proxy_conf = {"server": proxy_server}
            if user:
                proxy_conf.update({"username": user, "password": password})

        browser = p.chromium.launch(headless=headless, args=["--ignore-certificate-errors"])
        context = browser.new_context(proxy=proxy_conf, ignore_https_errors=True)
        page = context.new_page()

        page.goto(VINTED_URL, wait_until='domcontentloaded', timeout=60000)
        time.sleep(random.uniform(2, 4))
        page.goto(VINTED_URL + 'catalog', wait_until='domcontentloaded', timeout=60000)
        time.sleep(random.uniform(1, 3))

        cookies = context.cookies()
        # Retrieve UA via evaluate
        user_agent = page.evaluate("() => navigator.userAgent")

        sess = requests.Session()
        if proxy:
            if not proxy.startswith('http'):
                proxy = 'http://' + proxy
            sess.proxies.update({'http': proxy, 'https': proxy})
            sess.verify = False
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        for c in cookies:
            sess.cookies.set(c['name'], c['value'], domain=c.get('domain'), path=c.get('path', '/'))

        sess.headers.update({
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'fr-FR,fr;q=0.9,en;q=0.8',
            'user-agent': user_agent,
            'x-money-object': 'true',
            'referer': VINTED_URL
        })

        browser.close()
        return sess


def test_api_fetch(session):
    r = session.get(API_TEST, timeout=30)
    return r.status_code, r.text[:200]


class PlaywrightVintedClient:
    def __init__(self, proxy: str | None = None, headless: bool = True, use_mobile_headers: bool = False):
        self._pw = None
        self._browser = None
        self._context = None
        self._page = None
        self._use_mobile_headers = use_mobile_headers

        self._proxy_conf = None
        if proxy:
            raw = proxy
            if raw.startswith('http://'):
                raw = raw[len('http://'):]
            user = None
            password = None
            if '@' in raw:
                creds, host = raw.split('@', 1)
                if ':' in creds:
                    user, password = creds.split(':', 1)
                proxy_server = 'http://' + host
            else:
                proxy_server = 'http://' + raw
            self._proxy_conf = {"server": proxy_server}
            if user:
                self._proxy_conf.update({"username": user, "password": password})

        # Launch & warm-up
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(headless=headless, args=["--ignore-certificate-errors"])
        self._context = self._browser.new_context(proxy=self._proxy_conf, ignore_https_errors=True)
        self._page = self._context.new_page()

        self._page.goto(VINTED_URL, wait_until='domcontentloaded', timeout=60000)
        time.sleep(random.uniform(2, 4))
        self._page.goto(VINTED_URL + 'catalog', wait_until='domcontentloaded', timeout=60000)
        time.sleep(random.uniform(1, 3))

    def fetch_catalog_items(self, page_nb: int, cat_id: int, brand_id: int, per_page: int = 96) -> dict:
        # Navigate to specific catalog/brand page to ensure proper cookies/tokens
        try:
            target = f"{VINTED_URL}catalog?catalog[]={cat_id}&brand_ids[]={brand_id}"
            self._page.goto(target, wait_until='domcontentloaded', timeout=60000)
            time.sleep(random.uniform(1, 2))
        except Exception:
            pass

        # Build query just like the requests version
        query = {
            "page": str(page_nb),
            "per_page": str(per_page),
            "time": str(int(time.time())),
            "search_text": "",
            "catalog_ids": str(cat_id),
            "order": "relevance",
            "catalog_from": "0",
            "size_ids": "",
            "brand_ids": str(brand_id),
            "status_ids": "",
            "color_ids": "",
            "material_ids": "",
            "currency": "EUR",
            "disable_search_saving": "false"
        }
        url = API_TEST.split('?')[0].replace('/api/v2/catalog/items', '/api/v2/catalog/items')
        final_url = f"https://www.vinted.fr/api/v2/catalog/items?{urlencode(query)}"

        # Execute fetch within the page context to preserve TLS/HTTP2/cookies
        script = """
            (url, useMobile) => {
              const getCookie = (name) => {
                const match = document.cookie.split('; ').find(r => r.startsWith(name + '='));
                return match ? decodeURIComponent(match.split('=')[1]) : null;
              };
              const anon = getCookie('anon_id');
              const csrf = (document.querySelector('meta[name="csrf-token"]')||{}).content || getCookie('csrf');
              const headers = {
                'accept': 'application/json, text/plain, */*',
                'x-money-object': 'true'
              };
              if (anon) headers['x-anon-id'] = anon;
              if (csrf) headers['x-csrf-token'] = csrf;
              if (useMobile) {
                headers['user-agent'] = 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36';
                headers['sec-ch-ua'] = '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"';
                headers['sec-ch-ua-mobile'] = '?1';
                headers['sec-ch-ua-platform'] = '"Android"';
              }
              return fetch(url, {
                method: 'GET',
                headers,
                credentials: 'include'
              }).then(async r => ({ status: r.status, json: await r.json().catch(() => ({})), text: await r.text().catch(() => '') }))
            }
        """
        result = self._page.evaluate(script, final_url, self._use_mobile_headers)
        status = result.get('status', 0)
        if status != 200:
            # Return status and body for diagnostics
            return {"status": status, "error": True, "json": result.get('json'), "text": result.get('text', '')}
        return result.get('json', {})

    def get_cookies_and_ua_session(self):
        cookies = self._context.cookies()
        user_agent = self._page.evaluate("() => navigator.userAgent")
        sess = requests.Session()
        for c in cookies:
            sess.cookies.set(c['name'], c['value'], domain=c.get('domain'), path=c.get('path', '/'))
        sess.headers.update({
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'fr-FR,fr;q=0.9,en;q=0.8',
            'user-agent': user_agent,
            'x-money-object': 'true',
            'referer': VINTED_URL
        })
        return sess

    def close(self):
        try:
            if self._browser:
                self._browser.close()
        finally:
            if self._pw:
                self._pw.stop()


if __name__ == '__main__':
    proxy = BRIGHT_PROXY
    print(f"Using proxy: {proxy}")
    # Basic smoke test: cookie/UA session
    sess = launch_and_get_session(proxy=proxy)
    code, preview = test_api_fetch(sess)
    print(f"API status: {code}")
    print(preview)
    # Playwright client direct fetch test
    client = PlaywrightVintedClient(proxy=proxy, headless=True)
    try:
        data = client.fetch_catalog_items(page_nb=1, cat_id=221, brand_id=115, per_page=10)
        if isinstance(data, dict) and data.get('items') is not None:
            print(f"Playwright fetch items: {len(data.get('items', []))}")
        else:
            print(f"Playwright fetch status/error: {data}")
    finally:
        client.close()
