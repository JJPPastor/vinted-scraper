#!/usr/bin/env python3
"""
Test script for the improved Vinted scraper with anti-detection measures
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from vinted_scraper import test_vinted_connection, cat_api_caller, create_new_session

def test_improved_scraper():
    """Test the improved scraper with anti-detection measures"""
    print("🧪 Testing Improved Vinted Scraper")
    print("=" * 50)
    
    # Test 1: Connection
    print("\n1. Testing connection to Vinted...")
    if not test_vinted_connection():
        print("❌ Connection test failed")
        return False
    print("✅ Connection test passed")
    
    # Test 2: Session creation
    print("\n2. Testing session creation...")
    try:
        session = create_new_session()
        print("✅ Session creation successful")
    except Exception as e:
        print(f"❌ Session creation failed: {e}")
        return False
    
    # Test 3: Single API request with new session
    print("\n3. Testing single API request with improved session...")
    df, success = cat_api_caller(1, 221, 115, session)  # category_id=221, brand_id=115 (Sandro)
    
    if success and len(df) > 0:
        print(f"✅ Success! Retrieved {len(df)} items")
        print(f"Sample item: {df.iloc[0]['title']}")
        return True
    else:
        print("❌ Failed to retrieve data")
        return False

if __name__ == "__main__":
    success = test_improved_scraper()
    sys.exit(0 if success else 1) 