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

'''
ssl._create_default_https_context = ssl._create_unverified_context
opener = urllib.request.build_opener(
    urllib.request.ProxyHandler(
        {'http': 'http://brd-customer-hl_44a8fa04-zone-unblocker:5aef1i6b54iw@brd.superproxy.io:22225',
        'https': 'http://brd-customer-hl_44a8fa04-zone-unblocker:5aef1i6b54iw@brd.superproxy.io:22225'}))
'''

vinted_taxonomy = pd.read_csv('../data/vinted_taxonomy.csv')  

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
    
    # Simulate browser session for the new session
    if not simulate_browser_session(session):
        print("Failed to simulate browser session for new session")
        return None
        
    return session

def handle_rate_limiting(session, retry_count=0, max_retries=3):
    """
    Handle rate limiting by implementing exponential backoff
    """
    if retry_count >= max_retries:
        print("Max retries reached for rate limiting")
        return False
    
    wait_time = 60 * (2 ** retry_count)  # Exponential backoff: 60s, 120s, 240s
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
    # Try direct connection first (local IP)
    print("Testing direct connection (local IP)...")
    if test_direct_connection():
        print("✅ Successfully connected to Vinted directly")
        return True
    
    # If direct connection fails, try with proxy
    print("Direct connection failed, trying proxy...")
    proxy = get_working_proxy()
    if proxy:
        print(f"Testing connection with proxy: {proxy}")
        session = requests.Session()
        session.proxies.update({'http': proxy, 'https': proxy})
        
        # Disable SSL verification for proxy connections (only for regular sessions)
        session.verify = False
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
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
        
        try:
            response = session.get("https://www.vinted.fr/", timeout=30)
            if response.status_code == 200:
                print("✅ Successfully connected to Vinted with proxy")
                return True
            else:
                print(f"❌ Failed to connect to Vinted with proxy: HTTP {response.status_code}")
        except Exception as e:
            print(f"❌ Error connecting to Vinted with proxy: {e}")
    
    print("❌ Failed to connect to Vinted with any method")
    return False

def simulate_browser_session(session):
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
            
        # Step 3: Visit a specific category page to get category-specific tokens
        time.sleep(random.uniform(1, 3))
        category_response = session.get("https://www.vinted.fr/catalog?catalog[]=221", timeout=30)
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

def cat_api_caller(page_nb, cat_id, brand_id, session=None):    
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
        
        # Add random delay before first request
        time.sleep(random.uniform(1, 3))
        
        # Simulate a real browser session
        if not simulate_browser_session(session):
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
        if handle_rate_limiting(session):
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
            print(f"Completed {row['category_name']} ({category_pages} pages)\n")
        else:
            print(f"Failed to collect data for {row['category_name']}\n")
            
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
    Test if the proxy connection is working
    """
    try:
        proxies = {'http': proxy, 'https': proxy}
        session = requests.Session()
        session.proxies.update(proxies)
        
        # Disable SSL verification for proxy testing
        session.verify = False
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        response = session.get("https://www.vinted.fr/", timeout=30)
        if response.status_code == 200:
            print(f"✅ Proxy test successful: {proxy}")
            return True
        else:
            print(f"❌ Proxy test failed: HTTP {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Proxy test error: {e}")
        return False

# Global cache for working proxy
_working_proxy_cache = None
_proxy_cache_time = 0
PROXY_CACHE_DURATION = 300  # 5 minutes

def invalidate_proxy_cache():
    """
    Invalidate the proxy cache to force a new proxy test
    """
    global _working_proxy_cache, _proxy_cache_time
    _working_proxy_cache = None
    _proxy_cache_time = 0
    print("Proxy cache invalidated")

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
        # Your existing Bright Data proxies
        'http://brd-customer-hl_44a8fa04-zone-data_center:8ht957dpf6o0@brd.superproxy.io:33335',
        'http://brd-customer-hl_44a8fa04-zone-unblocker:5aef1i6b54iw@brd.superproxy.io:22225',
        
        # Free proxy options (less reliable but worth trying)
        'http://185.199.229.156:7492',
        'http://185.199.228.220:7492',
        'http://185.199.231.45:7492',
        'http://185.199.230.102:7492',
        
        # Add more proxy options here
    ]
    
    for proxy in proxy_options:
        print(f"Testing proxy: {proxy}")
        if test_proxy_connection(proxy):
            _working_proxy_cache = proxy
            _proxy_cache_time = current_time
            print(f"Cached working proxy: {proxy}")
            return proxy
    
    print("❌ No working proxies found")
    _working_proxy_cache = None
    return None

    
    
if __name__ == '__main__':
    # Test connection first
    if not test_vinted_connection():
        print("Cannot connect to Vinted. Please check your internet connection or try again later.")
        exit(1)
    
    brand_id = 130332 ## Balzac
    brand_id = 115 ##Sandro
    #brand_id = 121 ## Isabel Marant
    
    # Use auto-resume feature
    print("Starting Vinted scraper with auto-resume feature...")
    df = full_vinted_cat_api_caller(brand_id, auto_resume=True)
    
    print(f"Collection completed. Total items collected: {len(df)}")
    #print(df)
    #df, cot =cat_api_caller(1, 221, 130332)
    #print(df)
    #for i,row in vinted_taxonomy.iterrows():
    #    print(cat_name_finder(row['category_id']))




### Iterate over all categories (they only have one id, no hierarchy, so need to get all lowest level ones)




