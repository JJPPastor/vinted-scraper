#!/usr/bin/env python3
"""
Vinted Scraper with Rotating Proxy Support
"""

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
import os
import glob

# Initialize fake user agent
try:
    ua = UserAgent()
except:
    ua = None

def get_random_user_agent():
    """Get a random user agent"""
    if ua:
        return ua.random
    else:
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ]
        return random.choice(user_agents)

def get_proxy_list():
    """
    Get a list of proxies to rotate through
    You can replace this with your own proxy list
    """
    # Example proxy list - replace with your actual proxies
    proxies = [
        # Free proxies (not recommended for production)
        # "http://proxy1:port",
        # "http://proxy2:port",
        
        # Paid proxy services (recommended)
        # Bright Data, Oxylabs, SmartProxy, etc.
        
        # For testing, you can use free proxies but they're unreliable
        # "http://185.199.229.156:7492",
        # "http://185.199.228.220:7492",
        # "http://185.199.231.45:7492",
        
        # Or use a proxy service API
        # "http://username:password@proxy-service.com:port"
    ]
    
    # If you don't have proxies, return None to use direct connection
    return proxies if proxies else None

def create_session_with_proxy(proxy=None):
    """
    Create a session with optional proxy
    """
    try:
        # Try cloudscraper first
        session = cloudscraper.create_scraper(
            browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False}
        )
        print("Using cloudscraper with proxy" if proxy else "Using cloudscraper without proxy")
    except Exception as e:
        print(f"Cloudscraper failed, using regular session: {e}")
        session = requests.Session()
    
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
    
    # Add proxy if provided
    if proxy:
        session.proxies.update({
            'http': proxy,
            'https': proxy
        })
        print(f"Using proxy: {proxy}")
    
    return session

def test_proxy(proxy):
    """
    Test if a proxy is working
    """
    try:
        session = create_session_with_proxy(proxy)
        response = session.get("https://www.vinted.fr/", timeout=30)
        return response.status_code == 200
    except:
        return False

def get_working_proxy():
    """
    Get a working proxy from the list
    """
    proxy_list = get_proxy_list()
    
    if not proxy_list:
        print("No proxy list provided, using direct connection")
        return None
    
    print(f"Testing {len(proxy_list)} proxies...")
    
    for proxy in proxy_list:
        print(f"Testing proxy: {proxy}")
        if test_proxy(proxy):
            print(f"✅ Working proxy found: {proxy}")
            return proxy
        else:
            print(f"❌ Proxy failed: {proxy}")
    
    print("❌ No working proxies found, using direct connection")
    return None

def detect_last_position(brand_id):
    """
    Detect the last processed category and page from existing CSV files
    """
    pattern = f"../data/vinted_tests/raw_data/{brand_id}_*.csv"
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
        estimated_pages = len(df[df['category_id'] == last_category_id]) // 96
        
        print(f"Detected last position: Category {last_category_id} ({last_category_name}), ~{estimated_pages} pages")
        
        return last_category_id, last_category_name, estimated_pages
        
    except Exception as e:
        print(f"Error detecting last position: {e}")
        return None, None, 0

def simulate_browser_session_with_proxy(session):
    """
    Simulate browser session with proxy support
    """
    try:
        print("Simulating browser session with proxy...")
        
        # Step 1: Visit main page
        time.sleep(random.uniform(2, 4))
        main_response = session.get("https://www.vinted.fr/", timeout=30)
        if main_response.status_code != 200:
            print(f"Failed to access main page: {main_response.status_code}")
            return False
            
        # Step 2: Visit search page
        time.sleep(random.uniform(1, 3))
        search_response = session.get("https://www.vinted.fr/catalog", timeout=30)
        if search_response.status_code != 200:
            print(f"Failed to access search page: {search_response.status_code}")
            return False
            
        print("✅ Browser session simulation completed")
        return True
        
    except Exception as e:
        print(f"Error in browser session simulation: {e}")
        return False

def cat_api_caller_with_proxy(page_nb, cat_id, brand_id, session=None, proxy=None):
    """
    API caller with proxy support
    """
    # Create session if not provided
    if session is None:
        session = create_session_with_proxy(proxy)
        
        # Simulate browser session
        if not simulate_browser_session_with_proxy(session):
            return pd.DataFrame(), False
    
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
    
    # Set referer
    if page_nb == 1:
        referer = 'https://www.vinted.fr/'
    else:
        referer = f'https://www.vinted.fr/catalog?time={current_timestamp}&catalog[]={cat_id}&catalog_from=0&page={page_nb-1}&brand_ids[]={brand_id}'
    
    session.headers.update({"referer": referer})
    
    try:
        # Add random delay
        time.sleep(random.uniform(1, 3))
        
        response = session.get(url, params=querystring, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            
            if not data.get('items') or len(data['items']) == 0:
                print('No more items')
                return pd.DataFrame(), False
            
            # Convert to DataFrame (simplified)
            items = data['items']
            df = pd.DataFrame(items)
            df['category_id'] = cat_id
            df['category_name'] = cat_name_finder(cat_id)
            
            return df, True
            
        elif response.status_code == 403:
            print(f"403 Forbidden - IP might be blocked, consider rotating proxy")
            return pd.DataFrame(), False
        else:
            print(f"HTTP {response.status_code}")
            return pd.DataFrame(), False
            
    except Exception as e:
        print(f"Request error: {e}")
        return pd.DataFrame(), False

def cat_name_finder(cat_id):
    """Find category name by ID"""
    # This would need to be implemented based on your taxonomy
    return f"category_{cat_id}"

def vinted_scraper_with_proxy_rotation(brand_id, start_id=None, total_page_nb=None, auto_resume=True):
    """
    Vinted scraper with proxy rotation to avoid IP blocking
    """
    # Auto-detect last position
    if auto_resume and start_id is None and total_page_nb is None:
        detected_cat_id, detected_cat_name, detected_pages = detect_last_position(brand_id)
        if detected_cat_id is not None:
            print(f"Auto-resuming from category {detected_cat_id} ({detected_cat_name})")
            start_id = detected_cat_id
            total_page_nb = detected_pages
    
    # Get working proxy
    proxy = get_working_proxy()
    
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
    
    # Load taxonomy
    vinted_taxonomy = pd.read_csv('../data/vinted_taxonomy.csv')
    
    consecutive_failures = 0
    max_consecutive_failures = 3
    
    for i, row in vinted_taxonomy.iterrows():
        # Skip to start_id if specified
        if start_id != None:
            if row['category_id'] != start_id:
                print(f"Skipping {row['category_id']} (looking for {start_id})")
                continue
            else:
                print(f"Found start_id {start_id}, beginning collection")
                start_id = None
        
        print(f'Started {row["category_name"]}')
        category_success = False
        category_pages = 0
        
        # Collect pages for this category
        for page_num in range(1, 11):
            print(f"Collecting Page {page_num} of {row['category_name']}")
            
            # Try with current proxy
            temp_df, continuation = cat_api_caller_with_proxy(page_num, row['category_id'], brand_id, None, proxy)
            
            if continuation and len(temp_df) > 0:
                full_df = pd.concat([full_df, temp_df])
                pages_collected += 1
                category_pages += 1
                
                # Save progress
                full_df.to_csv(f'../data/vinted_tests/raw_data/{brand_id}_{pages_collected}.csv')
                
                consecutive_failures = 0
                category_success = True
                print(f"Successfully collected {len(temp_df)} items from page {page_num}")
            else:
                consecutive_failures += 1
                print(f"Failed to get data for page {page_num}")
                
                # If too many failures, try rotating proxy
                if consecutive_failures >= max_consecutive_failures:
                    print(f"Too many consecutive failures ({consecutive_failures}), trying new proxy...")
                    proxy = get_working_proxy()
                    consecutive_failures = 0
                    time.sleep(random.uniform(30, 60))
                else:
                    time.sleep(random.uniform(15, 30))
            
            # If no more items, break
            if not continuation:
                print(f"No more items for {row['category_name']}")
                break
        
        if category_success:
            print(f"Completed {row['category_name']} ({category_pages} pages)\n")
        else:
            print(f"Failed to collect data for {row['category_name']}\n")
            
    return full_df

if __name__ == '__main__':
    print("🚀 Starting Vinted Scraper with Proxy Rotation...")
    
    brand_id = 115  # Sandro
    df = vinted_scraper_with_proxy_rotation(brand_id, auto_resume=True)
    
    print(f"✅ Collection completed. Total items collected: {len(df)}") 