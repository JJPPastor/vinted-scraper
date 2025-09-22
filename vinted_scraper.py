import requests
import json 
import pandas as pd
from datetime import datetime  
import time 
import random 
import ssl
import urllib.request
from fake_useragent import UserAgent
import cloudscraper
from pathlib import Path
import os
import glob
import uuid
import re
import argparse

# Optional curl-cffi HTTP/2 + TLS mimic
try:
    from curl_cffi import requests as curl_requests
    _CURL_AVAILABLE = True
except Exception:
    _CURL_AVAILABLE = False

# Optional Playwright client import
try:
    from scrapers.vinted_scraper_playwright import PlaywrightVintedClient
except Exception:
    try:
        from vinted_scraper_playwright import PlaywrightVintedClient
    except Exception:
        PlaywrightVintedClient = None

# Resolve paths relative to repo root
REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_DATA_DIR = (REPO_ROOT / 'data' / 'vinted_tests' / 'raw_data')
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR = (REPO_ROOT / 'data' / 'logs' / 'vinted')
LOGS_DIR.mkdir(parents=True, exist_ok=True)
RAW_JSON_DIR = (REPO_ROOT / 'data' / 'vinted_tests' / 'raw_json')
RAW_JSON_DIR.mkdir(parents=True, exist_ok=True)
STATE_DIR = (REPO_ROOT / 'data' / 'vinted_tests' / 'state')
STATE_DIR.mkdir(parents=True, exist_ok=True)
VINTED_TAXONOMY_PATH = (REPO_ROOT / 'data' / 'vinted_taxonomy.csv')

# Initialize fake user agent
try:
    ua = UserAgent()
except:
    # Fallback user agents if fake_useragent fails
    ua = None

def get_random_user_agent():
    """Get a random user agent"""
    if ua:
        return ua.random
    else:
        # Fallback user agents
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ]
        return random.choice(user_agents)

def _infer_platform_from_ua(user_agent: str):
    ua = user_agent.lower()
    if 'windows nt' in ua:
        return 'Windows', False
    if 'android' in ua:
        return 'Android', True
    if 'iphone' in ua or 'ipad' in ua or 'ios' in ua:
        return 'iOS', True
    if 'macintosh' in ua or 'mac os x' in ua:
        return 'macOS', False
    if 'linux' in ua:
        return 'Linux', False
    return 'Windows', False

def build_client_hints_headers(user_agent: str) -> dict:
    platform, is_mobile = _infer_platform_from_ua(user_agent)
    # Keep Chromium-like ch-ua; versions are not strictly validated server-side
    sec_ch_ua = '"Not)A;Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"'
    return {
        "sec-ch-ua": sec_ch_ua,
        "sec-ch-ua-mobile": "?1" if is_mobile else "?0",
        "sec-ch-ua-platform": f'"{platform}"'
    }

def ensure_anon_id(session: requests.Session) -> bool:
    """Set x-anon-id header only if cookie anon_id exists; otherwise remove it.
    Returns True if header set, False otherwise."""
    try:
        cookie_anon = session.cookies.get("anon_id")
    except Exception:
        cookie_anon = None
    if cookie_anon:
        session.headers.update({"x-anon-id": cookie_anon})
        return True
    # Remove any stale header
    if 'x-anon-id' in session.headers:
        session.headers.pop('x-anon-id', None)
    return False

def log_response(prefix: str, response: requests.Response, note: str = "") -> None:
    try:
        ts = int(time.time())
        fname = LOGS_DIR / f"{prefix}_{response.status_code}_{ts}.log"
        body = ""
        try:
            body = response.text
        except Exception:
            body = "<no-text>"
        body = body[:4000]
        with open(fname, 'w', encoding='utf-8') as f:
            f.write(f"{note}\n")
            f.write(f"URL: {getattr(response.request, 'url', '')}\n")
            f.write(f"METHOD: {getattr(response.request, 'method', '')}\n")
            f.write(f"REQ_HEADERS: {getattr(response.request, 'headers', {})}\n")
            f.write(f"RESP_HEADERS: {dict(response.headers)}\n")
            f.write(f"STATUS: {response.status_code}\n\n")
            f.write(body)
    except Exception:
        pass

def save_raw_json(payload: dict, brand_id: int, cat_id: int, page_nb: int) -> None:
    try:
        ts = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        target_dir = RAW_JSON_DIR / str(brand_id) / str(cat_id)
        target_dir.mkdir(parents=True, exist_ok=True)
        out = target_dir / f"{ts}_p{page_nb}.json"
        with open(out, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False)
    except Exception:
        pass

def _state_path(brand_id: int, cat_id: int) -> Path:
    return STATE_DIR / f"{brand_id}_{cat_id}.json"

def read_last_seen(brand_id: int, cat_id: int):
    try:
        p = _state_path(brand_id, cat_id)
        if not p.exists():
            return None
        with open(p, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data.get('last_seen_item_id')
    except Exception:
        return None

def write_last_seen(brand_id: int, cat_id: int, last_seen_id) -> None:
    try:
        p = _state_path(brand_id, cat_id)
        with open(p, 'w', encoding='utf-8') as f:
            json.dump({
                'last_seen_item_id': last_seen_id,
                'updated_at': datetime.utcnow().isoformat()
            }, f)
    except Exception:
        pass

_pw_client_singleton = None

def get_playwright_client(proxy: str | None):
    global _pw_client_singleton
    if PlaywrightVintedClient is None:
        return None
    if _pw_client_singleton is None:
        try:
            _pw_client_singleton = PlaywrightVintedClient(proxy=proxy, headless=True)
        except Exception:
            _pw_client_singleton = None
    return _pw_client_singleton

def reset_playwright_client(proxy: str | None):
    global _pw_client_singleton
    try:
        if _pw_client_singleton is not None:
            _pw_client_singleton.close()
    except Exception:
        pass
    _pw_client_singleton = None
    return get_playwright_client(proxy)

'''
# Legacy paid proxy example removed
# ssl._create_default_https_context = ssl._create_unverified_context
# opener = urllib.request.build_opener(
#     urllib.request.ProxyHandler({
#         'http': 'http://USER:PASS@HOST:PORT',
#         'https': 'http://USER:PASS@HOST:PORT'
#     })
# )
'''

vinted_taxonomy = pd.read_csv(VINTED_TAXONOMY_PATH)

def cat_name_finder(cat_id):
    return vinted_taxonomy[vinted_taxonomy['category_id']==cat_id]['category_name'].to_list()[0]
    
def vinted_api_to_df(json_data):
    """
    Convert Vinted API response to pandas DataFrame
    
    Args:
        json_data: Either a JSON string or dictionary containing Vinted API response
    
    Returns:
        pandas.DataFrame: Flattened DataFrame with product information
    """
    # Handle both string and dict inputs
    if isinstance(json_data, str):
        data = json.loads(json_data)
    else:
        data = json_data
    
    # Extract items array
    items = data.get('items', [])
    
    # List to store flattened item data
    flattened_items = []
    
    for item in items:
        flattened_item = {
            'id': item.get('id'),
            'title': item.get('title'),
            'price_amount': item.get('price', {}).get('amount'),
            'price_currency': item.get('price', {}).get('currency_code'),
            'is_visible': item.get('is_visible'),
            'brand_title': item.get('brand_title'),
            'path': item.get('path'),
            'url': item.get('url'),
            'promoted': item.get('promoted'),
            'favourite_count': item.get('favourite_count'),
            'is_favourite': item.get('is_favourite'),
            'service_fee_amount': item.get('service_fee', {}).get('amount'),
            'service_fee_currency': item.get('service_fee', {}).get('currency_code'),
            'total_item_price_amount': item.get('total_item_price', {}).get('amount'),
            'total_item_price_currency': item.get('total_item_price', {}).get('currency_code'),
            'view_count': item.get('view_count'),
            'size_title': item.get('size_title'),
            'content_source': item.get('content_source'),
            'status': item.get('status'),
            
            # User information
            'user_id': item.get('user', {}).get('id'),
            'user_login': item.get('user', {}).get('login'),
            'user_profile_url': item.get('user', {}).get('profile_url'),
            'user_business': item.get('user', {}).get('business'),
            
            # Main photo information
            'photo_id': item.get('photo', {}).get('id') if item.get('photo') else None,
            'photo_width': item.get('photo', {}).get('width') if item.get('photo') else None,
            'photo_height': item.get('photo', {}).get('height') if item.get('photo') else None,
            'photo_url': item.get('photo', {}).get('url') if item.get('photo') else None,
            'photo_dominant_color': item.get('photo', {}).get('dominant_color') if item.get('photo') else None,
            'photo_is_main': item.get('photo', {}).get('is_main') if item.get('photo') else None,
            
            # Item box information
            'item_box_first_line': item.get('item_box', {}).get('first_line'),
            'item_box_second_line': item.get('item_box', {}).get('second_line'),
            'item_box_accessibility_label': item.get('item_box', {}).get('accessibility_label'),
            'item_box_item_id': item.get('item_box', {}).get('item_id'),
            
            # Search tracking
            'search_score': item.get('search_tracking_params', {}).get('score'),
        }
        
        flattened_items.append(flattened_item)
    
    # Create DataFrame
    df = pd.DataFrame(flattened_items)
    
    # Convert numeric columns to appropriate types
    numeric_columns = ['price_amount', 'service_fee_amount', 'total_item_price_amount', 
                      'favourite_count', 'view_count', 'photo_width', 'photo_height', 'search_score']
    
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df

def create_new_session():
    """
    Create a new session with different parameters to avoid detection
    """
    try:
        # Try cloudscraper with different browser profile
        browsers = [
            {'browser': 'chrome', 'platform': 'windows', 'mobile': False},
            {'browser': 'firefox', 'platform': 'windows', 'mobile': False},
            {'browser': 'chrome', 'platform': 'darwin', 'mobile': False},
            {'browser': 'firefox', 'platform': 'darwin', 'mobile': False}
        ]
        
        browser_config = random.choice(browsers)
        session = cloudscraper.create_scraper(browser=browser_config)
        print(f"Created new cloudscraper session with {browser_config}")
    except Exception as e:
        print(f"Cloudscraper failed, using regular session: {e}")
        session = requests.Session()
    
    # Set up headers with new user agent
    user_agent = get_random_user_agent()
    session.headers.update({
        "accept": "application/json, text/plain, */*",
        "accept-language": "fr-FR,fr;q=0.9,en;q=0.8",
        **build_client_hints_headers(user_agent),
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": user_agent,
        "x-money-object": "true",
        "dnt": "1",
        "upgrade-insecure-requests": "1",
        "cache-control": "no-cache",
        "pragma": "no-cache"
    })
    
    # Simulate browser session for the new session
    if not simulate_browser_session(session):
        print("Failed to simulate browser session for new session")
        return None
        
    return session

def handle_rate_limiting(session, retry_count=0, max_retries=3, response=None):
    """
    Handle rate limiting by implementing exponential backoff
    """
    if retry_count >= max_retries:
        print("Max retries reached for rate limiting")
        return False
    
    wait_time = 60 * (2 ** retry_count)  # Exponential backoff default
    # Use Retry-After header if present
    try:
        if response is not None:
            ra = response.headers.get('Retry-After')
            if ra:
                if re.fullmatch(r"\d+", ra):
                    wait_time = max(wait_time, int(ra))
                else:
                    # HTTP-date format
                    from email.utils import parsedate_to_datetime
                    ra_dt = parsedate_to_datetime(ra)
                    wait_time = max(wait_time, int((ra_dt - datetime.utcnow()).total_seconds()))
    except Exception:
        pass
    print(f"Rate limited. Waiting {wait_time} seconds before retry {retry_count + 1}/{max_retries}...")
    time.sleep(wait_time)
    
    # Refresh cookies after waiting
    if refresh_session_cookies(session):
        return True
    else:
        return handle_rate_limiting(session, retry_count + 1, max_retries)

def test_vinted_connection():
    """
    Test the connection to Vinted to ensure we can access the site
    """
    # Proxy-only checks
    proxy = get_working_proxy()
    if not proxy:
        print("❌ No working proxy available (free proxy pool empty).")
        return False

    # Final fallback: try Playwright session (browser-context)
    try:
        pw = get_playwright_client(get_working_proxy())
        if pw is not None:
            # Use cookie-seeded requests session to test API endpoint
            sess = pw.get_cookies_and_ua_session()
            r = sess.get("https://www.vinted.fr/api/v2/catalog/items?page=1&per_page=1", timeout=30)
            if r.status_code == 200:
                print("✅ Playwright context can access Vinted")
                return True
            else:
                print(f"❌ Playwright context failed: HTTP {r.status_code}")
    except Exception as e:
        print(f"❌ Playwright fallback error: {e}")
    print("❌ Failed to connect to Vinted via proxy methods")
    return False

def simulate_browser_session(session, cat_id: int | None = None, brand_id: int | None = None):
    """
    Simulate a real browser session by visiting pages in the correct order
    """
    try:
        print("Simulating browser session...")
        
        # Step 1: Visit main page
        time.sleep(random.uniform(2, 4))
        main_response = session.get("https://www.vinted.fr/", timeout=30)
        if main_response.status_code != 200:
            print(f"Failed to access main page: {main_response.status_code}")
            return False
            
        # Step 2: Visit search page to get search tokens
        time.sleep(random.uniform(1, 3))
        search_response = session.get("https://www.vinted.fr/catalog", timeout=30)
        if search_response.status_code != 200:
            print(f"Failed to access search page: {search_response.status_code}")
            return False
            
        # Step 3: Visit a specific category/brand page to get category-specific tokens
        time.sleep(random.uniform(1, 3))
        target_cat = 221 if cat_id is None else cat_id
        url = f"https://www.vinted.fr/catalog?catalog[]={target_cat}"
        if brand_id is not None:
            url += f"&brand_ids[]={brand_id}"
        category_response = session.get(url, timeout=30)
        if category_response.status_code != 200:
            print(f"Failed to access category page: {category_response.status_code}")
            return False
            
        # Step 4: Visit the API endpoint directly to get API tokens
        time.sleep(random.uniform(1, 2))
        api_test_response = session.get("https://www.vinted.fr/api/v2/catalog/items?page=1&per_page=20", timeout=30)
        if api_test_response.status_code != 200:
            print(f"Failed to access API endpoint: {api_test_response.status_code}")
            # Don't fail completely, just warn
            print("Warning: API endpoint test failed, but continuing...")
            
        print("Browser session simulation completed")
        return True
        
    except Exception as e:
        print(f"Error simulating browser session: {e}")
        return False

def refresh_session_cookies(session):
    """
    Refresh session cookies by visiting the main page and following some navigation
    """
    try:
        # Add random delay
        time.sleep(random.uniform(2, 5))
        
        # Visit main page
        main_response = session.get("https://www.vinted.fr/", timeout=30)
        if main_response.status_code != 200:
            print(f"Failed to access main page: {main_response.status_code}")
            # Try with different approach
            try:
                # Try with different headers
                session.headers.update({
                    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                    "sec-fetch-dest": "document",
                    "sec-fetch-mode": "navigate",
                    "sec-fetch-site": "none",
                    "sec-fetch-user": "?1"
                })
                main_response = session.get("https://www.vinted.fr/", timeout=30)
                if main_response.status_code != 200:
                    print(f"Still failed to access main page: {main_response.status_code}")
                    return False
            except Exception as e:
                print(f"Second attempt failed: {e}")
                return False
            
        # Add delay between requests
        time.sleep(random.uniform(1, 3))
            
        # Visit catalog page to get additional cookies
        catalog_response = session.get("https://www.vinted.fr/catalog", timeout=30)
        if catalog_response.status_code != 200:
            print(f"Failed to access catalog page: {catalog_response.status_code}")
            # Don't fail completely, just warn
            print("Warning: Could not access catalog page, continuing anyway")
            
        print("Session cookies refreshed successfully")
        return True
        
    except Exception as e:
        print(f"Error refreshing cookies: {e}")
        return False

def _requests_like_get(session, url, params=None, timeout=30):
    if _CURL_AVAILABLE:
        try:
            # Use Chromium JA3/tls fingerprint; http2=True defaults to h2
            with curl_requests.Session(impersonate="chrome", http2=True) as s:
                s.headers.update(session.headers)
                if getattr(session, 'proxies', None):
                    s.proxies.update(session.proxies)
                resp = s.get(url, params=params, timeout=timeout)
                class _R:
                    pass
                r = _R()
                r.status_code = resp.status_code
                r.headers = resp.headers
                r.text = resp.text
                def _json():
                    try:
                        return resp.json()
                    except Exception:
                        return {}
                r.json = _json
                r.request = type('rq', (), { 'url': resp.url, 'method': 'GET', 'headers': s.headers })
                return r
        except Exception:
            pass
    return session.get(url, params=params, timeout=timeout)

def cat_api_caller(page_nb, cat_id, brand_id, session=None, use_playwright=True, proxy=None, order: str | None = None, save_raw: bool = True):    
    # Create a session if not provided
    if session is None:
        # Try to use cloudscraper first, fallback to regular session
        try:
            session = cloudscraper.create_scraper(
                browser={
                    'browser': 'chrome',
                    'platform': 'windows',
                    'mobile': False
                }
            )
            print("Using cloudscraper for anti-bot protection")
        except Exception as e:
            print(f"Cloudscraper failed, using regular session: {e}")
            session = requests.Session()
        
        # Set up session with proper headers
        user_agent = get_random_user_agent()
        session.headers.update({
            "accept": "application/json, text/plain, */*",
            "accept-language": "fr-FR,fr;q=0.9,en;q=0.8",
            **build_client_hints_headers(user_agent),
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": user_agent,
            "x-money-object": "true",
            "dnt": "1",
            "upgrade-insecure-requests": "1",
            "cache-control": "no-cache",
            "pragma": "no-cache"
        })
        
        # Add random delay before first request
        time.sleep(random.uniform(1, 3))
        
        # Simulate a real browser session
        if not simulate_browser_session(session, cat_id=cat_id, brand_id=brand_id):
            return pd.DataFrame(), False

    # Configure proxy if needed
    proxy = get_working_proxy()
    if proxy:
        proxies = {'http': proxy, 'https': proxy}
        session.proxies.update(proxies)
        
        # Handle SSL verification based on session type
        if hasattr(session, 'cloudflareChallenge'):
            # This is a cloudscraper session - don't modify SSL settings
            print(f"Using proxy with cloudscraper: {proxy}")
        else:
            # This is a regular requests session - disable SSL verification
            session.verify = False
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            print(f"Using proxy with regular session: {proxy}")
    else:
        print("Using direct connection (local IP)")
    
    url = "https://www.vinted.fr/api/v2/catalog/items"
    
    # Build query parameters
    current_timestamp = int(datetime.today().timestamp())
    querystring = {
        "page": str(page_nb),
        "per_page": "96",
        "time": str(current_timestamp),
        "search_text": "",
        "catalog_ids": str(cat_id),
        "order": (order or "relevance"),
        "catalog_from": "0",
        "size_ids": "",
        "brand_ids": str(brand_id),
        "status_ids": "",
        "color_ids": "",
        "material_ids": ""
    }
    
    # Set referer based on page number (use specific catalog/brand page even for first page)
    base_ref = f'https://www.vinted.fr/catalog?time={current_timestamp}&catalog[]={cat_id}&catalog_from=0&brand_ids[]={brand_id}'
    referer = base_ref if page_nb == 1 else f'{base_ref}&page={page_nb-1}'
    
    # Update session headers for this request
    # Update session headers for this request and add Origin
    session.headers.update({
        "referer": referer,
        "origin": "https://www.vinted.fr"
    })
    
    # Handle anon_id cookie carefully to avoid conflicts
    ensure_anon_id(session)
    
    # Make the API request with retry logic
    max_retries = 3
    response = None
    
    for retry in range(max_retries):
        try:
            if use_playwright and PlaywrightVintedClient is not None:
                for attempt_pw in range(2):
                    try:
                        pw = get_playwright_client(proxy)
                        if pw is None:
                            break
                        data = pw.fetch_catalog_items(page_nb, cat_id, brand_id, per_page=96)
                        if isinstance(data, dict) and data.get('items') is not None:
                            df = vinted_api_to_df(data)
                            df['category_id'] = cat_id
                            df['category_name'] = cat_name_finder(cat_id)
                            return df, True
                        else:
                            print(f"Playwright fetch failed or blocked (attempt {attempt_pw+1}): {str(data)[:200]}")
                    except Exception as e:
                        print(f"Playwright error (attempt {attempt_pw+1}): {e}")
                    # Reset playwright and retry once on error/empty
                    if attempt_pw == 0:
                        reset_playwright_client(proxy)
                        time.sleep(random.uniform(1, 2))
                        continue
                    break
                # fall through to requests mode
            response = _requests_like_get(session, url, params=querystring, timeout=30)
            break  # Success, exit retry loop
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            if retry < max_retries - 1:
                print(f"Request failed (attempt {retry + 1}/{max_retries}): {e}")
                time.sleep(2 ** retry)  # Exponential backoff
                continue
            else:
                print(f"Request failed after {max_retries} attempts: {e}")
                return pd.DataFrame(), False
    
    # Check for different response codes
    if response.status_code == 403:
        print(f"403 Forbidden - Access denied. Response: {response.text[:200]}")
        log_response("403", response, note="Blocked at catalog endpoint")
        
        # Invalidate proxy cache since we got blocked
        invalidate_proxy_cache()
        
        # Try proxy rotation first
        print("Trying proxy rotation...")
        new_proxy = get_working_proxy(force_test=True)
        if new_proxy:
            print(f"Switching to new proxy: {new_proxy}")
            session.proxies.update({'http': new_proxy, 'https': new_proxy})
            response = session.get(url, params=querystring, timeout=30)
            if response.status_code == 200:
                print("Proxy rotation successful")
            else:
                print("Proxy rotation failed, trying anti-blocking measures...")
                if handle_persistent_blocking(session):
                    print("Applied anti-blocking measures, retrying...")
                    response = session.get(url, params=querystring, timeout=30)
                    if response.status_code == 403:
                        print("Still blocked after all measures")
                        return pd.DataFrame(), False
                else:
                    print("Failed to apply anti-blocking measures")
                    return pd.DataFrame(), False
        else:
            print("No new proxy available, trying anti-blocking measures...")
            if handle_persistent_blocking(session):
                print("Applied anti-blocking measures, retrying...")
                response = session.get(url, params=querystring, timeout=30)
                if response.status_code == 403:
                    print("Still blocked after anti-blocking measures")
                    return pd.DataFrame(), False
            else:
                print("Failed to apply anti-blocking measures")
                return pd.DataFrame(), False
    elif response.status_code == 429:
        print(f"429 Too Many Requests - Rate limited.")
        log_response("429", response, note="Rate limited at catalog endpoint")
        if handle_rate_limiting(session, response=response):
            return pd.DataFrame(), True  # Try again
        else:
            return pd.DataFrame(), False
    elif response.status_code == 401:
        print(f"401 Unauthorized - Authentication required. Response: {response.text[:200]}")
        # Try to refresh the session and retry
        print("Attempting to refresh session...")
        session = create_new_session()
        if session:
            response = session.get(url, params=querystring, timeout=30)
            if response.status_code == 401:
                print("Still getting 401 after session refresh")
                return pd.DataFrame(), False
        else:
            return pd.DataFrame(), False
    elif response.status_code != 200:
        print(f"HTTP {response.status_code} - {response.text[:200]}")
        return pd.DataFrame(), False
        
    # Parse JSON response
    data = response.json()
    if save_raw:
        try:
            save_raw_json(data, brand_id, cat_id, page_nb)
        except Exception:
            pass
    
    if not data.get('items') or len(data['items']) == 0:
        print('No more items')
        return pd.DataFrame(), False
    
    # Convert to DataFrame
    df = vinted_api_to_df(data)
    df['category_id'] = cat_id
    df['category_name'] = cat_name_finder(cat_id)
    
    return df, True

def run_brand_category_collection(brand_id: int, category_ids: list[int], pages: int = 10, mode: str = 'delta', use_playwright: bool = True, order: str | None = None):
    session = create_robust_session()
    if session is None:
        print("Failed to create a working session. Exiting.")
        return pd.DataFrame()
    full_df = pd.DataFrame()
    for cat_id in category_ids:
        print(f"Collecting brand {brand_id}, category {cat_id} in {mode} mode")
        last_seen = read_last_seen(brand_id, cat_id) if mode == 'delta' else None
        new_top_id = None
        stop = False
        for page in range(1, pages + 1):
            df, cont = cat_api_caller(page, cat_id, brand_id, session=session, use_playwright=use_playwright, order=order)
            if df is None or len(df) == 0:
                print("No data, stopping")
                break
            if page == 1:
                try:
                    new_top_id = int(df['id'].iloc[0])
                except Exception:
                    new_top_id = None
            if last_seen is not None:
                try:
                    if int(last_seen) in set(df['id'].astype(int).tolist()):
                        print("Reached last-seen item, stopping delta crawl for this category")
                        stop = True
                except Exception:
                    pass
            full_df = pd.concat([full_df, df])
            if stop or not cont:
                break
            time.sleep(random.uniform(0.4, 1.2))
        if mode == 'delta' and new_top_id is not None:
            write_last_seen(brand_id, cat_id, new_top_id)
    return full_df

def parse_category_list_arg(cats_arg: str) -> list[int]:
    """Parse comma-separated category IDs or the keyword 'all'.
    Accepts float-like tokens (e.g., '221.0')."""
    if not cats_arg:
        return []
    if cats_arg.strip().lower() == 'all':
        try:
            return sorted(list(pd.Series(vinted_taxonomy['category_id']).dropna().astype(int).unique()))
        except Exception:
            # Fallback: try coercing via float then int
            vals = []
            for x in pd.Series(vinted_taxonomy['category_id']).dropna().tolist():
                try:
                    vals.append(int(float(x)))
                except Exception:
                    continue
            return sorted(list(set(vals)))
    result: list[int] = []
    for tok in cats_arg.split(','):
        t = tok.strip()
        if not t:
            continue
        try:
            val = int(t) if '.' not in t else int(float(t))
            result.append(val)
        except Exception:
            print(f"Skipping invalid category token: {t}")
            continue
    return result

def full_vinted_cat_api_caller(brand_id, start_id=None, total_page_nb=None, auto_resume=True):
    """
    Main function to collect Vinted data with automatic resumption
    """
    # Auto-detect last position if not specified
    if auto_resume and start_id is None and total_page_nb is None:
        detected_cat_id, detected_cat_name, detected_pages = detect_last_position(brand_id)
        if detected_cat_id is not None:
            print(f"Auto-resuming from category {detected_cat_id} ({detected_cat_name})")
            start_id = detected_cat_id
            
            # Get the total page number from the latest file
            import glob
            import os
            pattern = f"../data/vinted_tests/raw_data/{brand_id}_*.csv"
            files = glob.glob(pattern)
            if files:
                latest_file = max(files, key=os.path.getctime)
                filename = os.path.basename(latest_file)
                try:
                    total_page_nb = int(filename.split('_')[1].split('.')[0])
                    print(f"Resuming from total page {total_page_nb}")
                except:
                    total_page_nb = detected_pages
            else:
                total_page_nb = detected_pages
    
    # Create robust session
    session = create_robust_session()
    if session is None:
        print("Failed to create a working session. Exiting.")
        return pd.DataFrame()
    
    # Initialize data
    if start_id == None:
        full_df = pd.DataFrame()
        pages_collected = 0
    else: 
        try:
            full_df = pd.read_csv(f'../data/vinted_tests/raw_data/{brand_id}_{total_page_nb}.csv')
            pages_collected = total_page_nb
            print(f"Loaded existing data: {len(full_df)} items")
        except FileNotFoundError:
            full_df = pd.DataFrame()
            pages_collected = 0
            print("No existing data found, starting fresh")
    
    consecutive_failures = 0
    max_consecutive_failures = 3
    session_failures = 0
    max_session_failures = 5
    
    # Track categories for better logging
    total_categories = len(vinted_taxonomy)
    processed_categories = 0
    
    for i, row in vinted_taxonomy.iterrows():
        # Skip to start_id if specified
        if start_id != None:
            if row['category_id'] != start_id:
                print(f"Skipping {row['category_id']} (looking for {start_id})")
                continue
            else:
                print(f"Found start_id {start_id}, beginning collection")
                start_id = None  # Reset so we process all subsequent categories
        
        processed_categories += 1
        print(f'Started {row["category_name"]} ({processed_categories}/{total_categories})')
        category_success = False
        category_pages = 0
        
        # Try to collect pages for this category
        for page_num in range(1, 11):  # Max 10 pages per category
            print(f"Collecting Page {page_num} of {row['category_name']}")
            
            # Try the request with retries
            for retry in range(3):
                temp_df, continuation = cat_api_caller(page_num, row['category_id'], brand_id, session)
                
                if continuation and len(temp_df) > 0:
                    full_df = pd.concat([full_df, temp_df])
                    pages_collected += 1
                    category_pages += 1
                    
                    # Save progress
                    full_df.to_csv(f'../data/vinted_tests/raw_data/{brand_id}_{pages_collected}.csv')
                    
                    consecutive_failures = 0  # Reset failure counter
                    category_success = True
                    print(f"Successfully collected {len(temp_df)} items from page {page_num}")
                    break
                else:
                    consecutive_failures += 1
                    print(f"Failed to get data for page {page_num} (attempt {retry + 1}/3)")
                    
                    if retry < 2:  # Not the last retry
                        time.sleep(random.uniform(5, 10))
                    else:  # Last retry failed
                        if consecutive_failures >= max_consecutive_failures:
                            print(f"Too many consecutive failures ({consecutive_failures}), creating new session...")
                            session = create_robust_session()
                            if session is None:
                                session_failures += 1
                                if session_failures >= max_session_failures:
                                    print("Too many session failures, stopping collection")
                                    return full_df
                            consecutive_failures = 0
                            time.sleep(random.uniform(30, 60))
                        else:
                            time.sleep(random.uniform(15, 30))
            
            # If we got no items, we've reached the end for this category
            if not continuation:
                print(f"No more items for {row['category_name']}")
                break
        
        if category_success:
            print(f"✅ Completed {row['category_name']} ({category_pages} pages)\n")
        else:
            print(f"❌ Failed to collect data for {row['category_name']} (no items found)\n")
            
        # Add delay between categories to avoid rate limiting
        time.sleep(random.uniform(2, 5))
    
    print(f"\n🎯 Collection Summary:")
    print(f"Total categories processed: {processed_categories}/{total_categories}")
    print(f"Total items collected: {len(full_df)}")
    print(f"Total pages collected: {pages_collected}")
            
    return full_df

def handle_persistent_blocking(session):
    """
    Handle persistent blocking by implementing more sophisticated measures
    """
    print("Handling persistent blocking...")
    
    # Try different strategies
    strategies = [
        # Strategy 1: Change user agent and headers
        lambda: change_session_identity(session),
        # Strategy 2: Use different request pattern
        lambda: change_request_pattern(session),
        # Strategy 3: Add delays and randomization
        lambda: add_randomization(session)
    ]
    
    for i, strategy in enumerate(strategies):
        try:
            print(f"Trying strategy {i + 1}...")
            if strategy():
                print(f"Strategy {i + 1} applied successfully")
                return True
        except Exception as e:
            print(f"Strategy {i + 1} failed: {e}")
    
    return False

def change_session_identity(session):
    """Change session identity to avoid detection"""
    # Change user agent
    new_user_agent = get_random_user_agent()
    session.headers.update({
        "user-agent": new_user_agent,
        "accept": "application/json, text/plain, */*",
        "accept-language": "fr-FR,fr;q=0.9,en;q=0.8",
        "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "x-money-object": "true",
        "dnt": "1",
        "upgrade-insecure-requests": "1",
        "cache-control": "no-cache",
        "pragma": "no-cache"
    })
    
    # Clear cookies and start fresh
    session.cookies.clear()
    return True

def change_request_pattern(session):
    """Change request pattern to avoid detection"""
    # Add more realistic headers
    session.headers.update({
        "accept-encoding": "gzip, deflate, br",
        "accept": "application/json, text/plain, */*",
        "connection": "keep-alive",
        "host": "www.vinted.fr"
    })
    return True

def add_randomization(session):
    """Add randomization to avoid detection"""
    # Add random delay
    time.sleep(random.uniform(10, 20))
    
    # Add random headers
    random_headers = {
        "x-requested-with": "XMLHttpRequest",
        "x-forwarded-for": f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}"
    }
    session.headers.update(random_headers)
    return True

def detect_last_position(brand_id):
    """
    Detect the last processed category and page from existing CSV files
    """
    import glob
    import os
    
    # Look for existing CSV files for this brand
    pattern = f"../data/vinted_tests/raw_data/{brand_id}_*.csv"
    files = glob.glob(pattern)
    
    if not files:
        return None, None, 0
    
    # Get the latest file
    latest_file = max(files, key=os.path.getctime)
    
    try:
        # Read the latest file to get the last category
        df = pd.read_csv(latest_file)
        
        if len(df) == 0:
            return None, None, 0
        
        # Get the last category processed
        last_category_id = df['category_id'].iloc[-1]
        last_category_name = df['category_name'].iloc[-1]
        
        # Count pages for this category
        category_df = df[df['category_id'] == last_category_id]
        estimated_pages = len(category_df) // 96  # Assuming 96 items per page
        
        print(f"Detected last position: Category {last_category_id} ({last_category_name}), ~{estimated_pages} pages")
        
        return last_category_id, last_category_name, estimated_pages
        
    except Exception as e:
        print(f"Error detecting last position: {e}")
        return None, None, 0

def create_robust_session(max_attempts=5):
    """
    Create a robust session with multiple fallback strategies
    """
    # Get working proxy first
    proxy = get_working_proxy()
    
    for attempt in range(max_attempts):
        try:
            print(f"Creating session attempt {attempt + 1}/{max_attempts}")
            
            # Try different approaches
            if attempt == 0:
                # Try cloudscraper first
                session = cloudscraper.create_scraper(
                    browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False}
                )
                print("Using cloudscraper (Chrome/Windows)")
            elif attempt == 1:
                # Try different browser profile
                session = cloudscraper.create_scraper(
                    browser={'browser': 'firefox', 'platform': 'darwin', 'mobile': False}
                )
                print("Using cloudscraper (Firefox/Mac)")
            elif attempt == 2:
                # Try mobile profile
                session = cloudscraper.create_scraper(
                    browser={'browser': 'chrome', 'platform': 'android', 'mobile': True}
                )
                print("Using cloudscraper (Chrome/Android)")
            else:
                # Fallback to regular session
                session = requests.Session()
                print("Using regular requests session")
            
            # Set up headers
            user_agent = get_random_user_agent()
            session.headers.update({
                "accept": "application/json, text/plain, */*",
                "accept-language": "fr-FR,fr;q=0.9,en;q=0.8",
                **build_client_hints_headers(user_agent),
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "user-agent": user_agent,
                "x-money-object": "true",
                "dnt": "1",
                "upgrade-insecure-requests": "1",
                "cache-control": "no-cache",
                "pragma": "no-cache"
            })
            # Align optional params seen in real calls
            session.params = session.params or {}
            session.params.update({
                "currency": "EUR",
                "disable_search_saving": "false"
            })
            
            # Add proxy if available
            if proxy:
                session.proxies.update({'http': proxy, 'https': proxy})
                
                # Handle SSL verification based on session type
                if hasattr(session, 'cloudflareChallenge'):
                    # This is a cloudscraper session - don't modify SSL settings
                    print(f"Using proxy with cloudscraper: {proxy}")
                else:
                    # This is a regular requests session - disable SSL verification
                    session.verify = False
                    import urllib3
                    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                    print(f"Using proxy with regular session: {proxy}")
            
            # Test the session
            if test_session(session):
                print(f"Session created successfully on attempt {attempt + 1}")
                return session
            else:
                print(f"Session test failed on attempt {attempt + 1}")
                
        except Exception as e:
            print(f"Session creation failed on attempt {attempt + 1}: {e}")
    
    print("Failed to create a working session after all attempts")
    return None

def test_session(session):
    """
    Test if a session can access Vinted
    """
    try:
        response = session.get("https://www.vinted.fr/", timeout=30)
        return response.status_code == 200
    except:
        return False

def test_proxy_connection(proxy):
    """
    Test if the proxy connection is working using neutral endpoints.
    """
    try:
        proxies = {'http': proxy, 'https': proxy}
        session = requests.Session()
        session.proxies.update(proxies)
        session.verify = False
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        # Fast no-content test
        r1 = session.get("https://www.google.com/generate_204", timeout=10)
        if r1.status_code not in (204, 200):
            return False
        # Check a JSON echo endpoint
        r2 = session.get("https://httpbin.org/ip", timeout=10)
        if r2.status_code != 200:
            return False
        return True
    except Exception:
        return False

# Global cache for working proxy
_working_proxy_cache = None
_proxy_cache_time = 0
PROXY_CACHE_DURATION = 300  # 5 minutes

def _fetch_free_proxies() -> list[str]:
    """
    Fetch a list of fresh free HTTP proxies from public sources.
    Returns a list like ["host:port", ...].
    """
    sources = [
        # Plain host:port per line
        "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
        "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt",
        "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
    ]
    results: list[str] = []
    for url in sources:
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200 and resp.text:
                for line in resp.text.splitlines():
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    if '://' in line:
                        # Normalize http://host:port → host:port
                        try:
                            line = line.split('://', 1)[1]
                        except Exception:
                            continue
                    # Filter out auth-bearing proxies (paid)
                    if '@' in line:
                        continue
                    # Basic format host:port
                    if ':' in line and line.count(':') == 1:
                        results.append(line)
        except Exception:
            continue
    # Deduplicate while preserving order
    seen = set()
    uniq: list[str] = []
    for p in results:
        if p not in seen:
            seen.add(p)
            uniq.append(p)
    return uniq[:500]

def invalidate_proxy_cache():
    """
    Invalidate the proxy cache to force a new proxy test
    """
    global _working_proxy_cache, _proxy_cache_time
    _working_proxy_cache = None
    _proxy_cache_time = 0
    print("Proxy cache invalidated")

def test_direct_connection():
    # Disabled: proxy-only mode
    return False

def get_working_proxy(force_test=False):
    """
    Get a working free proxy with caching. Paid proxies and auth-based endpoints are ignored.
    """
    global _working_proxy_cache, _proxy_cache_time
    current_time = time.time()

    # Return cached proxy if it's still valid
    if not force_test and _working_proxy_cache and (current_time - _proxy_cache_time) < PROXY_CACHE_DURATION:
        print(f"Using cached working proxy: {_working_proxy_cache}")
        return _working_proxy_cache

    candidates = _fetch_free_proxies()
    if not candidates:
        print("❌ Could not fetch free proxies.")
        return None

    # Try a handful quickly
    max_to_try = 20
    for raw in candidates[:max_to_try]:
        proxy = raw if raw.startswith('http') else f"http://{raw}"
        print(f"Testing free proxy: {proxy}")
        if test_proxy_connection(proxy):
            _working_proxy_cache = proxy
            _proxy_cache_time = current_time
            print(f"Cached working proxy: {proxy}")
            return proxy

    print("❌ No working free proxies found right now")
    _working_proxy_cache = None
    return None

    
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Vinted brand/category collector')
    parser.add_argument('--brand', type=int, help='Brand ID (required for targeted run)')
    parser.add_argument('--cats', type=str, help='Comma-separated category IDs or "all"')
    parser.add_argument('--pages', type=int, default=10, help='Max pages per category')
    parser.add_argument('--mode', type=str, default='delta', choices=['delta', 'full'], help='Collection mode')
    parser.add_argument('--use-playwright', action='store_true', help='Use Playwright fetch path')
    parser.add_argument('--order', type=str, default=None, help='Order param (e.g., newest_first)')
    parser.add_argument('--auto', action='store_true', help='Run legacy auto-resume over full taxonomy')
    args = parser.parse_args()

    if not test_vinted_connection():
        print("Cannot connect directly via basic checks; proceeding with Playwright if available...")
        # We do not exit here because Playwright fallback may still work

    if args.auto:
        # Legacy full taxonomy run
        brand_id = args.brand if args.brand else 765
        print("Starting Vinted scraper with auto-resume feature...")
        df = full_vinted_cat_api_caller(brand_id, auto_resume=True)
        print(f"Collection completed. Total items collected: {len(df)}")
    else:
        if not args.brand or not args.cats:
            print('Please provide --brand and --cats (comma-separated) or use --auto')
            exit(1)
        cats = parse_category_list_arg(args.cats)
        df = run_brand_category_collection(args.brand, cats, pages=args.pages, mode=args.mode, use_playwright=args.use_playwright or True, order=args.order)
        print(f"Completed targeted run. Total items collected: {len(df)}")




### Iterate over all categories (they only have one id, no hierarchy, so need to get all lowest level ones)




