#!/usr/bin/env python3
"""
Test script for the auto-resume feature
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from vinted_scraper import detect_last_position, test_vinted_connection

def test_auto_resume():
    """Test the auto-resume feature"""
    print("🧪 Testing Auto-Resume Feature")
    print("=" * 40)
    
    # Test 1: Connection
    print("\n1. Testing connection to Vinted...")
    if not test_vinted_connection():
        print("❌ Connection test failed")
        return False
    print("✅ Connection test passed")
    
    # Test 2: Auto-detection
    print("\n2. Testing auto-detection of last position...")
    brand_id = 115  # Sandro
    detected_cat_id, detected_cat_name, detected_pages = detect_last_position(brand_id)
    
    if detected_cat_id is not None:
        print(f"✅ Auto-detection successful:")
        print(f"   - Last category: {detected_cat_id} ({detected_cat_name})")
        print(f"   - Estimated pages: {detected_pages}")
        return True
    else:
        print("ℹ️  No existing data found (this is normal for first run)")
        return True

if __name__ == "__main__":
    success = test_auto_resume()
    sys.exit(0 if success else 1) 