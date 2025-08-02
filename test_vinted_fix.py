#!/usr/bin/env python3
"""
Test script for the improved Vinted scraper
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from vinted_scraper import test_vinted_connection, cat_api_caller

def test_single_request():
    """Test a single API request"""
    print("Testing single API request...")
    
    # Test with a simple category and brand
    df, success = cat_api_caller(1, 221, 115)  # category_id=221, brand_id=115 (Sandro)
    
    if success:
        print(f"✅ Success! Retrieved {len(df)} items")
        if len(df) > 0:
            print(f"Sample item: {df.iloc[0]['title']}")
        return True
    else:
        print("❌ Failed to retrieve data")
        return False

def main():
    print("🧪 Testing Vinted Scraper Fixes")
    print("=" * 40)
    
    # Test 1: Connection
    print("\n1. Testing connection to Vinted...")
    if not test_vinted_connection():
        print("❌ Connection test failed")
        return False
    print("✅ Connection test passed")
    
    # Test 2: Single API request
    print("\n2. Testing single API request...")
    if not test_single_request():
        print("❌ API request test failed")
        return False
    print("✅ API request test passed")
    
    print("\n🎉 All tests passed! The scraper should now work without 403 errors.")
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 