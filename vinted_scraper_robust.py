#!/usr/bin/env python3
"""
Robust Vinted Scraper with Advanced Anti-Detection Measures
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

def create_stealth_session():
    """
    Create a stealth session that mimics real browser behavior
    """
    # Try multiple approaches
    approaches = [
        # Approach 1: Cloudscraper with Chrome
        lambda: cloudscraper.create_scraper(browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False}),
        # Approach 2: Cloudscraper with Firefox
        lambda: cloudscraper.create_scraper(browser={'browser': 'firefox', 'platform': 'darwin', 'mobile': False}),
        # Approach 3: Cloudscraper with mobile
        lambda: cloudscraper.create_scraper(browser={'browser': 'chrome', 'platform': 'android', 'mobile': True}),
        # Approach 4: Regular session with stealth headers
        lambda: requests.Session()
    ]
    
    for i, approach in enumerate(approaches):
        try:
            print(f"Trying session approach {i + 1}...")
            session = approach()
            
            # Set up stealth headers
            user_agent = get_random_user_agent()
            session.headers.update({
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "accept-language": "fr-FR,fr;q=0.9,en;q=0.8",
                "accept-encoding": "gzip, deflate, br",
                "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "sec-fetch-dest": "document",
                "sec-fetch-mode": "navigate",
                "sec-fetch-site": "none",
                "sec-fetch-user": "?1",
                "upgrade-insecure-requests": "1",
                "user-agent": user_agent,
                "cache-control": "max-age=0"
            })
            
            # Test the session
            if test_session_stealth(session):
                print(f"✅ Session approach {i + 1} successful")
                return session
            else:
                print(f"❌ Session approach {i + 1} failed")
                
        except Exception as e:
            print(f"❌ Session approach {i + 1} error: {e}")
    
    print("❌ All session approaches failed")
    return None

def test_session_stealth(session):
    """
    Test session with stealth approach
    """
    try:
        # Add random delay
        time.sleep(random.uniform(2, 5))
        
        # Try to access the main page
        response = session.get("https://www.vinted.fr/", timeout=30)
        return response.status_code == 200
    except:
        return False

def simulate_human_browsing(session):
    """
    Simulate realistic human browsing behavior
    """
    try:
        print("Simulating human browsing behavior...")
        
        # Step 1: Visit main page
        time.sleep(random.uniform(3, 6))
        main_response = session.get("https://www.vinted.fr/", timeout=30)
        if main_response.status_code != 200:
            print(f"Failed to access main page: {main_response.status_code}")
            return False
            
        # Step 2: Visit search page
        time.sleep(random.uniform(2, 4))
        search_response = session.get("https://www.vinted.fr/catalog", timeout=30)
        if search_response.status_code != 200:
            print(f"Failed to access search page: {search_response.status_code}")
            return False
            
        # Step 3: Visit a specific category
        time.sleep(random.uniform(2, 4))
        category_response = session.get("https://www.vinted.fr/catalog?catalog[]=221", timeout=30)
        if category_response.status_code != 200:
            print(f"Failed to access category page: {category_response.status_code}")
            return False
            
        print("✅ Human browsing simulation completed")
        return True
        
    except Exception as e:
        print(f"Error in human browsing simulation: {e}")
        return False

def make_api_request_stealth(session, url, params):
    """
    Make API request with stealth measures
    """
    try:
        # Add random delay
        time.sleep(random.uniform(1, 3))
        
        # Update headers for API request
        session.headers.update({
            "accept": "application/json, text/plain, */*",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "x-money-object": "true"
        })
        
        # Make the request
        response = session.get(url, params=params, timeout=30)
        
        # Handle different response codes
        if response.status_code == 200:
            return response, True
        elif response.status_code == 403:
            print("403 Forbidden - Applying stealth measures...")
            # Apply stealth measures
            session.headers.update({
                "user-agent": get_random_user_agent(),
                "x-forwarded-for": f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}"
            })
            session.cookies.clear()
            time.sleep(random.uniform(5, 10))
            
            # Retry
            response = session.get(url, params=params, timeout=30)
            return response, response.status_code == 200
        else:
            print(f"HTTP {response.status_code}")
            return response, False
            
    except Exception as e:
        print(f"Request error: {e}")
        return None, False

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

def robust_vinted_scraper(brand_id, start_id=None, total_page_nb=None, auto_resume=True):
    """
    Robust Vinted scraper with advanced anti-detection measures
    """
    # Auto-detect last position
    if auto_resume and start_id is None and total_page_nb is None:
        detected_cat_id, detected_cat_name, detected_pages = detect_last_position(brand_id)
        if detected_cat_id is not None:
            print(f"Auto-resuming from category {detected_cat_id} ({detected_cat_name})")
            start_id = detected_cat_id
            total_page_nb = detected_pages
    
    # Create stealth session
    session = create_stealth_session()
    if session is None:
        print("Failed to create stealth session. Exiting.")
        return pd.DataFrame()
    
    # Simulate human browsing
    if not simulate_human_browsing(session):
        print("Failed to simulate human browsing. Exiting.")
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
    
    # Load taxonomy
    vinted_taxonomy = pd.read_csv('../data/vinted_taxonomy.csv')
    
    consecutive_failures = 0
    max_consecutive_failures = 5
    
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
            
            # Build API request
            current_timestamp = int(datetime.today().timestamp())
            url = "https://www.vinted.fr/api/v2/catalog/items"
            params = {
                "page": str(page_num),
                "per_page": "96",
                "time": str(current_timestamp),
                "search_text": "",
                "catalog_ids": str(row['category_id']),
                "order": "relevance",
                "catalog_from": "0",
                "size_ids": "",
                "brand_ids": str(brand_id),
                "status_ids": "",
                "color_ids": "",
                "material_ids": ""
            }
            
            # Make request with stealth measures
            response, success = make_api_request_stealth(session, url, params)
            
            if success:
                try:
                    data = response.json()
                    if data.get('items') and len(data['items']) > 0:
                        # Convert to DataFrame (simplified)
                        items = data['items']
                        temp_df = pd.DataFrame(items)
                        temp_df['category_id'] = row['category_id']
                        temp_df['category_name'] = row['category_name']
                        
                        full_df = pd.concat([full_df, temp_df])
                        pages_collected += 1
                        category_pages += 1
                        
                        # Save progress
                        full_df.to_csv(f'../data/vinted_tests/raw_data/{brand_id}_{pages_collected}.csv')
                        
                        consecutive_failures = 0
                        category_success = True
                        print(f"Successfully collected {len(temp_df)} items from page {page_num}")
                    else:
                        print(f"No more items for {row['category_name']}")
                        break
                except Exception as e:
                    print(f"Error processing response: {e}")
                    consecutive_failures += 1
            else:
                consecutive_failures += 1
                print(f"Failed to get data for page {page_num}")
                
                if consecutive_failures >= max_consecutive_failures:
                    print(f"Too many consecutive failures ({consecutive_failures}), creating new session...")
                    session = create_stealth_session()
                    if session and simulate_human_browsing(session):
                        consecutive_failures = 0
                        time.sleep(random.uniform(30, 60))
                    else:
                        print("Failed to create new session, stopping collection")
                        return full_df
                else:
                    time.sleep(random.uniform(15, 30))
        
        if category_success:
            print(f"Completed {row['category_name']} ({category_pages} pages)\n")
        else:
            print(f"Failed to collect data for {row['category_name']}\n")
            
    return full_df

if __name__ == '__main__':
    print("🚀 Starting Robust Vinted Scraper...")
    
    brand_id = 115  # Sandro
    df = robust_vinted_scraper(brand_id, auto_resume=True)
    
    print(f"✅ Collection completed. Total items collected: {len(df)}") 