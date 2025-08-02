#!/usr/bin/env python3
"""
Vinted Scraper - Standalone Version
Self-contained scraper that doesn't require external taxonomy files
"""

import requests
import pandas as pd
import json
import time
import random
import cloudscraper
from datetime import datetime
from fake_useragent import UserAgent

# Brand ID to scrape
brand_id = 31  # Isabel Marant

# Vinted taxonomy data embedded directly in the script
VINTED_TAXONOMY_DATA = [
    {'category_id': 221, 'category_name': 't-shirts'},
    {'category_id': 222, 'category_name': 'shirts'},
    {'category_id': 224, 'category_name': 'long-sleeved-tops'},
    {'category_id': 227, 'category_name': 'tunics'},
    {'category_id': 14, 'category_name': 'camis'},
    {'category_id': 1045, 'category_name': 'turtlenecks'},
    {'category_id': 1043, 'category_name': 'blouses'},
    {'category_id': 534, 'category_name': 'vest-tops-and-tank-tops'},
    {'category_id': 1042, 'category_name': 'off-the-shoulder-tops'},
    {'category_id': 1835, 'category_name': 'bodysuits'},
    {'category_id': 1044, 'category_name': 'halterneck-tops'},
    {'category_id': 1041, 'category_name': 'crop-tops'},
    {'category_id': 223, 'category_name': 'short-sleeved-tops'},
    {'category_id': 228, 'category_name': 'other-tops-and-t-shirts'},
    {'category_id': 225, 'category_name': 'three-fourths-sleeve-tops'},
    {'category_id': 1837, 'category_name': 'peplum-tops'},
    {'category_id': 1071, 'category_name': 'wide-leg-trousers'},
    {'category_id': 525, 'category_name': 'leggings'},
    {'category_id': 187, 'category_name': 'tailored-trousers'},
    {'category_id': 1846, 'category_name': 'straight-leg-trousers'},
    {'category_id': 1070, 'category_name': 'cropped-trousers-and-chinos'},
    {'category_id': 184, 'category_name': 'leather-trousers'},
    {'category_id': 189, 'category_name': 'other-trousers'},
    {'category_id': 185, 'category_name': 'skinny-trousers'},
    {'category_id': 526, 'category_name': 'harem-pants'},
    {'category_id': 1844, 'category_name': 'skinny-jeans'},
    {'category_id': 1841, 'category_name': 'flared-jeans'},
    {'category_id': 1839, 'category_name': 'boyfriend-jeans'},
    {'category_id': 1845, 'category_name': 'straight-jeans'},
    {'category_id': 1843, 'category_name': 'ripped-jeans'},
    {'category_id': 1840, 'category_name': 'cropped-jeans'},
    {'category_id': 1842, 'category_name': 'high-waisted-jeans'},
    {'category_id': 1864, 'category_name': 'other'},
    {'category_id': 576, 'category_name': 'tops-and-t-shirts'},
    {'category_id': 572, 'category_name': 'tracksuits'},
    {'category_id': 574, 'category_name': 'dresses'},
    {'category_id': 571, 'category_name': 'outerwear'},
    {'category_id': 573, 'category_name': 'trousers'},
    {'category_id': 1439, 'category_name': 'sports-bras'},
    {'category_id': 575, 'category_name': 'skirts'},
    {'category_id': 580, 'category_name': 'other-activewear'},
    {'category_id': 578, 'category_name': 'shorts'},
    {'category_id': 579, 'category_name': 'sports-accessories'},
    {'category_id': 577, 'category_name': 'hoodies-and-sweatshirts'},
    {'category_id': 3268, 'category_name': 'team-shirts-and-jerseys'},
    {'category_id': 1131, 'category_name': 'jumpsuits'},
    {'category_id': 1132, 'category_name': 'playsuits'},
    {'category_id': 1134, 'category_name': 'other-jumpsuits-and-playsuits'},
    {'category_id': 1782, 'category_name': 'costumes-and-special-outfits'},
    {'category_id': 1065, 'category_name': 'summer-dresses'},
    {'category_id': 178, 'category_name': 'mini-dresses'},
    {'category_id': 1058, 'category_name': 'little-black-dresses'},
    {'category_id': 1779, 'category_name': 'winter-dresses'},
    {'category_id': 1055, 'category_name': 'long-dresses'},
    {'category_id': 1056, 'category_name': 'midi-dresses'},
    {'category_id': 1057, 'category_name': 'formal-and-work-dresses'},
    {'category_id': 1061, 'category_name': 'strapless-dresses'},
    {'category_id': 1774, 'category_name': 'special-occasion-dresses'},
    {'category_id': 1059, 'category_name': 'casual-dresses'},
    {'category_id': 176, 'category_name': 'other-dresses'},
    {'category_id': 179, 'category_name': 'denim-dresses'},
    {'category_id': 198, 'category_name': 'mini-skirts'},
    {'category_id': 199, 'category_name': 'midi-skirts'},
    {'category_id': 2927, 'category_name': 'knee-length-skirts'},
    {'category_id': 2929, 'category_name': 'skorts'},
    {'category_id': 200, 'category_name': 'maxi-skirts'},
    {'category_id': 2928, 'category_name': 'asymmetric-skirts'},
    {'category_id': 538, 'category_name': 'denim-shorts'},
    {'category_id': 1838, 'category_name': 'low-waisted-shorts'},
    {'category_id': 1103, 'category_name': 'cargo-shorts'},
    {'category_id': 1099, 'category_name': 'high-waisted-shorts'},
    {'category_id': 204, 'category_name': 'cropped-trousers'},
    {'category_id': 1101, 'category_name': 'lace-shorts'},
    {'category_id': 205, 'category_name': 'other-shorts-and-cropped-trousers'},
    {'category_id': 203, 'category_name': 'knee-length-shorts'},
    {'category_id': 1100, 'category_name': 'leather-shorts'},
    {'category_id': 119, 'category_name': 'bras'},
    {'category_id': 120, 'category_name': 'panties'},
    {'category_id': 1263, 'category_name': 'tights-and-stockings'},
    {'category_id': 1781, 'category_name': 'shapewear'},
    {'category_id': 123, 'category_name': 'nightwear'},
    {'category_id': 1847, 'category_name': 'lingerie-accessories'},
    {'category_id': 1030, 'category_name': 'dressing-gowns'},
    {'category_id': 229, 'category_name': 'sets'},
    {'category_id': 1262, 'category_name': 'socks'},
    {'category_id': 124, 'category_name': 'other'},
    {'category_id': 18, 'category_name': 'other-clothing'},
    {'category_id': 1917, 'category_name': 'jumpers'},
    {'category_id': 197, 'category_name': 'other-jumpers-and-sweaters'},
    {'category_id': 195, 'category_name': 'boleros'},
    {'category_id': 196, 'category_name': 'hoodies-and-sweatshirts'},
    {'category_id': 1874, 'category_name': 'waistcoats'},
    {'category_id': 194, 'category_name': 'cardigans'},
    {'category_id': 1067, 'category_name': 'kimonos'},
    {'category_id': 1908, 'category_name': 'jackets'},
    {'category_id': 1773, 'category_name': 'capes-and-ponchos'},
    {'category_id': 1907, 'category_name': 'coats'},
    {'category_id': 2524, 'category_name': 'gilets-and-body-warmers'},
    {'category_id': 219, 'category_name': 'bikinis-and-tankinis'},
    {'category_id': 220, 'category_name': 'one-pieces'},
    {'category_id': 218, 'category_name': 'cover-ups-and-sarongs'},
    {'category_id': 1780, 'category_name': 'blazers'},
    {'category_id': 532, 'category_name': 'other-suits-and-blazers'},
    {'category_id': 1129, 'category_name': 'trouser-suits'},
    {'category_id': 1125, 'category_name': 'trouser-suits'}
]

# Create DataFrame from embedded data
vinted_taxonomy = pd.DataFrame(VINTED_TAXONOMY_DATA)

# Global cache for working proxy
_working_proxy_cache = None
_proxy_cache_time = 0
PROXY_CACHE_DURATION = 300  # 5 minutes

def get_random_user_agent():
    """
    Get a random user agent string
    """
    try:
        ua = UserAgent()
        return ua.random
    except:
        # Fallback user agents if fake_useragent fails
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0"
        ]
        return random.choice(user_agents)

def cat_name_finder(cat_id):
    """
    Find category name by ID
    """
    category = vinted_taxonomy[vinted_taxonomy['category_id'] == cat_id]
    if len(category) > 0:
        return category['category_name'].iloc[0]
    return f"unknown-category-{cat_id}"

def vinted_api_to_df(json_data):
    """
    Convert Vinted API response to pandas DataFrame
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

def test_direct_connection():
    """
    Test if direct connection (local IP) works
    """
    try:
        session = requests.Session()
        session.headers.update({
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "fr-FR,fr;q=0.9,en;q=0.8",
            "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
        })
        
        response = session.get("https://www.vinted.fr/", timeout=30)
        if response.status_code == 200:
            print("✅ Direct connection (local IP) works")
            return True
        else:
            print(f"❌ Direct connection failed: HTTP {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Direct connection error: {e}")
        return False

def get_working_proxy(force_test=False):
    """
    Get a working proxy from available options with caching
    """
    global _working_proxy_cache, _proxy_cache_time
    
    current_time = time.time()
    
    # Return cached proxy if it's still valid
    if not force_test and _working_proxy_cache and (current_time - _proxy_cache_time) < PROXY_CACHE_DURATION:
        print(f"Using cached working proxy: {_working_proxy_cache}")
        return _working_proxy_cache
    
    # First try direct connection (local IP)
    print("Testing direct connection (local IP)...")
    if test_direct_connection():
        print("Using direct connection (no proxy needed)")
        _working_proxy_cache = None  # No proxy needed
        _proxy_cache_time = current_time
        return None  # Return None to indicate no proxy needed
    
    # If direct connection fails, try proxies
    print("Direct connection failed, trying proxies...")
    proxy_options = [
        'http://brd-customer-hl_44a8fa04-zone-data_center:8ht957dpf6o0@brd.superproxy.io:33335',
        'http://brd-customer-hl_44a8fa04-zone-unblocker:5aef1i6b54iw@brd.superproxy.io:22225',
    ]
    
    for proxy in proxy_options:
        print(f"Testing proxy: {proxy}")
        try:
            proxies = {'http': proxy, 'https': proxy}
            session = requests.Session()
            session.proxies.update(proxies)
            session.verify = False
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            
            response = session.get("https://www.vinted.fr/", timeout=30)
            if response.status_code == 200:
                print(f"✅ Proxy test successful: {proxy}")
                _working_proxy_cache = proxy
                _proxy_cache_time = current_time
                print(f"Cached working proxy: {proxy}")
                return proxy
            else:
                print(f"❌ Proxy test failed: HTTP {response.status_code}")
        except Exception as e:
            print(f"❌ Proxy test error: {e}")
    
    print("❌ No working proxies found")
    _working_proxy_cache = None
    return None

def invalidate_proxy_cache():
    """
    Invalidate the proxy cache to force a new proxy test
    """
    global _working_proxy_cache, _proxy_cache_time
    _working_proxy_cache = None
    _proxy_cache_time = 0
    print("Proxy cache invalidated")

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
                "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
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
            try:
                response = session.get("https://www.vinted.fr/", timeout=30)
                if response.status_code == 200:
                    print(f"Session created successfully on attempt {attempt + 1}")
                    return session
                else:
                    print(f"Session test failed on attempt {attempt + 1}")
            except Exception as e:
                print(f"Session test failed on attempt {attempt + 1}: {e}")
                
        except Exception as e:
            print(f"Session creation failed on attempt {attempt + 1}: {e}")
    
    print("Failed to create a working session after all attempts")
    return None

def detect_last_position(brand_id):
    """
    Detect the last processed category and page from saved CSV files
    """
    import glob
    import os
    
    pattern = f"data/vinted_tests/raw_data/{brand_id}_*.csv"
    files = glob.glob(pattern)
    
    if not files:
        return None, None, 0
    
    latest_file = max(files, key=os.path.getctime)
    
    try:
        df = pd.read_csv(latest_file)
        if len(df) == 0:
            return None, None, 0
        
        last_category_id = df['category_id'].iloc[-1]
        last_category_name = df['category_name'].iloc[-1]
        category_df = df[df['category_id'] == last_category_id]
        estimated_pages = len(category_df) // 96
        
        print(f"Detected last position: Category {last_category_id} ({last_category_name}), ~{estimated_pages} pages")
        return last_category_id, last_category_name, estimated_pages
    except Exception as e:
        print(f"Error detecting last position: {e}")
        return None, None, 0

def cat_api_caller(page_nb, cat_id, brand_id, session=None):
    """
    Call Vinted API for a specific category and page
    """
    # Create a session if not provided
    if session is None:
        session = create_robust_session()
        if session is None:
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
        "order": "relevance",
        "catalog_from": "0",
        "size_ids": "",
        "brand_ids": str(brand_id),
        "status_ids": "",
        "color_ids": "",
        "material_ids": ""
    }
    
    # Set referer based on page number
    if page_nb == 1:
        referer = 'https://www.vinted.fr/'
    else:
        referer = f'https://www.vinted.fr/catalog?time={current_timestamp}&catalog[]={cat_id}&catalog_from=0&page={page_nb-1}&brand_ids[]={brand_id}'
    
    # Update session headers for this request
    session.headers.update({
        "referer": referer
    })
    
    # Handle anon_id cookie carefully to avoid conflicts
    try:
        anon_id = session.cookies.get("anon_id", "04dfd096-880e-4a73-8905-ef7bc54d8272")
        session.headers.update({"x-anon-id": anon_id})
    except Exception:
        # If there are cookie conflicts, use default anon_id
        session.headers.update({"x-anon-id": "04dfd096-880e-4a73-8905-ef7bc54d8272"})
    
    # Make the API request with retry logic
    max_retries = 3
    response = None
    
    for retry in range(max_retries):
        try:
            response = session.get(url, params=querystring, timeout=30)
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
                print("Proxy rotation failed")
                return pd.DataFrame(), False
        else:
            print("No new proxy available")
            return pd.DataFrame(), False
    elif response.status_code == 429:
        print(f"429 Too Many Requests - Rate limited.")
        time.sleep(60)  # Wait 1 minute
        return pd.DataFrame(), True  # Try again
    elif response.status_code == 401:
        print(f"401 Unauthorized - Authentication required.")
        return pd.DataFrame(), False
    elif response.status_code != 200:
        print(f"HTTP {response.status_code} - {response.text[:200]}")
        return pd.DataFrame(), False
        
    # Parse JSON response
    data = response.json()
    
    if not data.get('items') or len(data['items']) == 0:
        print('No more items')
        return pd.DataFrame(), False
    
    # Convert to DataFrame
    df = vinted_api_to_df(data)
    df['category_id'] = cat_id
    df['category_name'] = cat_name_finder(cat_id)
    
    return df, True

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
            full_df = pd.read_csv(f'data/vinted_tests/raw_data/{brand_id}_{total_page_nb}.csv')
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
    
    for i, row in vinted_taxonomy.iterrows():
        # Skip to start_id if specified
        if start_id != None:
            if row['category_id'] != start_id:
                print(f"Skipping {row['category_id']} (looking for {start_id})")
                continue
            else:
                print(f"Found start_id {start_id}, beginning collection")
                start_id = None  # Reset so we process all subsequent categories
        
        print(f'Started {row["category_name"]}')
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
                    
                    # Create data directory if it doesn't exist
                    import os
                    os.makedirs('data/vinted_tests/raw_data', exist_ok=True)
                    
                    # Save progress
                    full_df.to_csv(f'data/vinted_tests/raw_data/{brand_id}_{pages_collected}.csv', index=False)
                    
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
                            consecutive_failures = 0
                            session_failures += 1
                            
                            if session_failures >= max_session_failures:
                                print("Too many session failures, stopping collection")
                                return full_df
                        
                        print(f"No more items for {row['category_name']}")
                        break
            
            if not continuation:
                break
        
        print(f"Completed {row['category_name']} ({category_pages} pages)")
        
        # Add delay between categories
        time.sleep(random.uniform(2, 5))
    
    print(f"Collection completed. Total items collected: {len(full_df)}")
    return full_df

if __name__ == "__main__":
    print("Starting Vinted scraper with auto-resume feature...")
    
    # Test connection first
    if not test_direct_connection():
        print("Cannot connect to Vinted. Please check your internet connection or try again later.")
        exit(1)
    
    # Run the scraper
    df = full_vinted_cat_api_caller(brand_id, auto_resume=True)
    print(f"Scraping completed. Total items: {len(df)}") 