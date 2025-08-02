import requests
import json 
import math
import pandas as pd
from typing import Any, Dict, List
import time
from datetime import datetime
import math
import asyncio
import numpy as np
import uuid
import random
import string

vc_taxo = pd.read_csv('../data/vestiaire_taxonomy.csv')
macro_vc_taxo = pd.read_csv('../data/macro_vestiaire_taxonomy.csv')

def generate_dynamic_headers():
    """
    Generate dynamic headers with fresh cookies and session IDs
    """
    # Generate a random device ID
    device_id = str(uuid.uuid4())
    
    # Generate a random search query ID
    search_query_id = str(uuid.uuid4())
    
    # Generate a random search session ID
    search_session_id = str(uuid.uuid4())
    
    # Generate a random CFUVID cookie with current timestamp
    timestamp = int(time.time() * 1000)
    cfuvid_suffix = ''.join(random.choices(string.ascii_letters + string.digits, k=20))
    cfuvid = f"{cfuvid_suffix}-{timestamp}-0.0.1.1-604800000"
    
    # Generate a random user agent (keeping mobile format)
    user_agents = [
        "Mozilla/5.0 (Linux; Android 10; SM-G975F Build/QP1A.190711.020) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Mobile Safari/537.36",
        "Mozilla/5.0 (Linux; Android 11; Pixel 5 Build/RQ3A.210805.001.A1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Mobile Safari/537.36",
        "Mozilla/5.0 (Linux; Android 12; SM-G991B Build/SP1A.210812.016) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Mobile Safari/537.36",
        "Mozilla/5.0 (Linux; Android 13; OnePlus 9 Pro Build/TKQ1.220829.002) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Mobile Safari/537.36"
    ]
    user_agent = random.choice(user_agents)
    
    headers = {
        "cookie": f"_cfuvid={cfuvid}",
        "accept": "application/json",
        "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
        "content-type": "application/json",
        "origin": "https://fr.vestiairecollective.com",
        "priority": "u=1, i",
        "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
        "sec-ch-ua-mobile": "?1",
        "sec-ch-ua-platform": '"Android"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": user_agent,
        "x-deviceid": device_id,
        "x-search-query-id": search_query_id,
        "x-search-session-id": search_session_id,
        "x-use-case": "plpStandard",
        "x-userid": ""
    }
    
    return headers

def make_request_with_retry(url, payload, referer, max_retries=3):
    """
    Make a request with retry mechanism and fresh headers for each attempt
    """
    for attempt in range(max_retries):
        try:
            # Generate fresh headers for each attempt
            headers = generate_dynamic_headers()
            headers["referer"] = referer
            
            # Add a small delay to avoid rate limiting
            time.sleep(random.uniform(1, 3))
            
            response = requests.request("POST", url, json=payload, headers=headers)
            
            # Check if response is successful
            if response.status_code == 200:
                return response
            
            # If not successful, wait longer before retry
            if attempt < max_retries - 1:
                time.sleep(random.uniform(5, 10))
                
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(random.uniform(5, 10))
    
    # If all retries failed, return the last response
    return response

def test_dynamic_headers():
    """
    Test function to verify that dynamic headers are working correctly
    """
    print("Testing dynamic headers generation...")
    
    # Test multiple header generations
    for i in range(3):
        headers = generate_dynamic_headers()
        print(f"Test {i+1}:")
        print(f"  Device ID: {headers['x-deviceid']}")
        print(f"  Search Query ID: {headers['x-search-query-id']}")
        print(f"  Search Session ID: {headers['x-search-session-id']}")
        print(f"  Cookie: {headers['cookie']}")
        print(f"  User Agent: {headers['user-agent']}")
        print()
    
    print("Dynamic headers test completed successfully!")

def sub_cat_name_finder(cat_id):
    try:
        cat_name = vc_taxo[vc_taxo['sub_category_id']==cat_id]['sub_category'].to_list()[0]
    except: 
        cat_name = None
    return cat_name

def parent_cat_name_finder(cat_id):
    try:
        cat_name = vc_taxo[vc_taxo['category_id']==cat_id]['category'].to_list()[0]
    except:
        cat_name = None
    return cat_name

def divide_and_round_up(number):
    return math.ceil(number / 60)

def flatten_json_to_df(data: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Convert nested JSON data to a flattened pandas DataFrame
    """
    flattened_records = []
    
    for record in data:
        flattened_record = {}
        
        # Flatten the record recursively
        def flatten_dict(obj: Any, parent_key: str = '', sep: str = '_') -> None:
            if isinstance(obj, dict):
                for key, value in obj.items():
                    new_key = f"{parent_key}{sep}{key}" if parent_key else key
                    flatten_dict(value, new_key, sep)
            elif isinstance(obj, list):
                # Handle lists - for colors.all, pictures, etc.
                if len(obj) == 0:
                    flattened_record[parent_key] = None
                elif len(obj) == 1 and isinstance(obj[0], dict):
                    # Single dict in list - flatten it
                    flatten_dict(obj[0], parent_key, sep)
                elif all(isinstance(item, dict) for item in obj):
                    # Multiple dicts in list - create indexed columns
                    for i, item in enumerate(obj):
                        flatten_dict(item, f"{parent_key}_{i}", sep)
                else:
                    # List of simple values - join them or take first
                    if all(isinstance(item, str) for item in obj):
                        flattened_record[parent_key] = '; '.join(obj)
                    else:
                        flattened_record[parent_key] = obj[0] if obj else None
            else:
                flattened_record[parent_key] = obj
        
        flatten_dict(record)
        flattened_records.append(flattened_record)
    
    # Create DataFrame
    df = pd.DataFrame(flattened_records)
    
    return df

def vc_api_call(brand_id, catalogLinksWithoutLanguage, page_nb):
    url = "https://search.vestiairecollective.com/v1/product/search"
    payload = {
            "pagination": {
                "offset": 60*page_nb,
                "limit": 60
            },
            "fields": ["name","condition","description", "brand", "model", "country", "price", "discount", "link", "sold", "likes", "editorPicks", "shouldBeGone", "seller", "directShipping", "local", "pictures", "colors", "size", "stock", "universeId", "createdAt"],
            "facets": {
                "fields": ["brand", "universe", "country", "stock", "color", "categoryLvl0", "priceRange", "price", "condition", "region", "editorPicks", "watchMechanism", "discount", "sold", "directShippingEligible", "directShippingCountries", "localCountries", "sellerBadge", "isOfficialStore", "materialLvl0", "size0", "size1", "size2", "size3", "size4", "size5", "size6", "size7", "size8", "size9", "size10", "size11", "size12", "size13", "size14", "size15", "size16", "size17", "size18", "size19", "size20", "size21", "size22", "size23", "model", "categoryLvl1", "categoryLvl2", "dealEligible"],
                "stats": ["price"]
            },
            "q": None,
            "sortBy": "relevance",
            "filters": {
                "brand.id": [f"{brand_id}"],
                "catalogLinksWithoutLanguage": [f"{catalogLinksWithoutLanguage}"]
            },
            "locale": {
                "country": "GB",
                "currency": "EUR",
                "language": "fr",
                "sizeType": "FR"
            },
            "mySizes": None,
            "options": {
                "innerFeedContext": "genericPLP",
                "disableHierarchicalParentFiltering": True
            },
            "recentlyViewedProductIDs": []
        }
        ##edit the referer if 
        
    if page_nb == 0: 
        referer = "https://fr.vestiairecollective.com/"
    else: 
        referer = f"https://fr.vestiairecollective.com{catalogLinksWithoutLanguage}p-{page_nb}/#brand={catalogLinksWithoutLanguage.replace('-','%20').replace('/','')}%23{brand_id}"
    
    # Generate dynamic headers
    headers = generate_dynamic_headers()
    try:
        response = make_request_with_retry(url, payload, referer)
        
        if page_nb == 0:
            item_nb = response.json()['facets']['fields']['brand'][0]['count']
            print(item_nb)
            total_pages = divide_and_round_up(item_nb)
            print(total_pages)
        
        data = response.json()['items']
        temp_df = flatten_json_to_df(data)
        #print(temp_df)
    except Exception as e:
        print(f"Error on page {page_nb}: {e}")
        print(response.text if 'response' in locals() else "No response")
        total_pages = 1
        temp_df = pd.DataFrame()
    
    if page_nb == 0:
        return total_pages, temp_df
    else: 
        return temp_df
    

def vestiaire_scraper(brand_id, catalogLinksWithoutLanguage):
    ### Makes initial API call to get the number of pages + first page data        
    total_pages, full_df = vc_api_call(brand_id, catalogLinksWithoutLanguage, 0)
    print(total_pages)
    
    if total_pages < 16:
        ### Iterates over number of pages 
        for i in range(1, total_pages):
            temp_df = vc_api_call(brand_id, catalogLinksWithoutLanguage, i)
            full_df = pd.concat([full_df, temp_df])
            full_df.to_csv(f"../data/vc_tests/{catalogLinksWithoutLanguage.replace('/','')}_{i}.csv")
    else: 
        ### need to iterate over categories to have maximum 1000 items per category
        ## create a catalog + naming for vestiaire co with associated id for each (and universe and lv1,2 3)
        pass
    full_df.to_csv(f'../data/{catalogLinksWithoutLanguage}_full.csv')
    
    return temp_df, full_df

def cat_api_caller(page_nb, brand_id, catalogLinksWithoutLanguage, universe_id, parent_cat_id, cat_id, sub_cat_id):
    url = "https://search.vestiairecollective.com/v1/product/search"
    cat_id = str(cat_id).split('.')[0]
    print(universe_id,parent_cat_id,cat_id)
    if pd.notna(sub_cat_id):
        #print('NA Ignored')
        filters = {
            "brand.id": [f"{brand_id}"],
            "catalogLinksWithoutLanguage": [f"{catalogLinksWithoutLanguage}"],
            "universe.id": [f"{universe_id}"],
            "categoryLvl0.id": [f"{parent_cat_id}"],
            "categoryLvl1.id": [f"{cat_id}"],
            "categoryLvl2.id": [f"{sub_cat_id}"]
        }
    else:
        if pd.notna(cat_id):
            #print('NA Not Ignored')
            filters = {
                "brand.id": [f"{brand_id}"],
                "catalogLinksWithoutLanguage": [f"{catalogLinksWithoutLanguage}"],
                "universe.id": [f"{universe_id}"],
                "categoryLvl0.id": [f"{parent_cat_id}"],
                "categoryLvl1.id": [f"{cat_id}"],
            }
        else: 
            filters = {
                "brand.id": [f"{brand_id}"],
                "catalogLinksWithoutLanguage": [f"{catalogLinksWithoutLanguage}"],
                "universe.id": [f"{universe_id}"],
                "categoryLvl0.id": [f"{parent_cat_id}"],
            }

    payload = {
        "pagination": {
            "offset": 60*page_nb,
            "limit": 60
        },
       "fields": ["name", "description", "brand", "model", "country", "price", "discount", "link", "sold", "likes", "editorPicks", "shouldBeGone", "seller", "directShipping", "local", "pictures", "colors", "size", "stock", "universeId", "createdAt"],
        "facets": {
            "fields": ["brand", "universe", "country", "stock", "color", "categoryLvl0", "priceRange", "price", "condition", "region", "editorPicks", "watchMechanism", "discount", "sold", "directShippingEligible", "directShippingCountries", "localCountries", "sellerBadge", "isOfficialStore", "materialLvl0", "size0", "size1", "size2", "size3", "size4", "size5", "size6", "size7", "size8", "size9", "size10", "size11", "size12", "size13", "size14", "size15", "size16", "size17", "size18", "size19", "size20", "size21", "size22", "size23", "model", "categoryLvl1", "categoryLvl2", "dealEligible"],
            "stats": ["price"]
        },
        "q": None,
        "sortBy": "recency",
        "filters": filters,
        "locale": {
            "country": "FR",
            "currency": "EUR",
            "language": "fr",
            "sizeType": "FR"
        },
        "mySizes": None,
        "options": {
            "innerFeedContext": "genericPLP",
            "disableHierarchicalParentFiltering": True
        },
    }
    try:
        response = make_request_with_retry(url, payload, "https://fr.vestiairecollective.com/")
        
        if page_nb == 0:
            item_nb = response.json()['facets']['fields']['brand'][0]['count']
            total_pages = divide_and_round_up(item_nb)

        data = response.json()['items']
        temp_df = flatten_json_to_df(data)
        try:
            try:
                sub_cat_name = sub_cat_name_finder(sub_cat_id)
            except:
                print('No Sub Cat Name Found')
                sub_cat_name = None
            temp_df['sub_category'] = sub_cat_name
            try:
                parent_cat_name = parent_cat_name_finder(cat_id)
            except: 
                print('No Parent Cat Name Found')
                parent_cat_name = None
            temp_df['parent_category'] = parent_cat_name
        except:
            print('sub_cat_issue')
    except Exception as e: 
        print(f"Error in cat_api_caller: {e}")
        print(response.text if 'response' in locals() else "No response")
        total_pages = 1
        temp_df = pd.DataFrame()
    
    if page_nb == 0:
        return total_pages, temp_df
    else: 
        return temp_df
    

def full_cat_vc_api_call(brand_id, catalogLinksWithoutLanguage, continuous=False, macro_taxo=False):
    full_df = pd.DataFrame()
    if macro_taxo:
        taxo = macro_vc_taxo
    else:
        taxo = vc_taxo
    for i, row in taxo.iterrows(): 
        print(f"Started Collecting: {row['universe']}, {row['parent_cat']}, {row['category']}, {row['sub_category']}.")
        total_pages, temp_df = cat_api_caller(0, brand_id, catalogLinksWithoutLanguage, row['universe_id'], row['parent_cat_id'], row['category_id'], row['sub_category_id'])
        full_df = pd.concat([full_df, temp_df])
        if total_pages == 1: 
            print(f"Finished Collecting: {row['universe']}, {row['parent_cat']}, {row['category']}, {row['sub_category']}.\n")
            continue
        else: 
            for i in range(1,total_pages):
                temp_df = cat_api_caller(i, brand_id, catalogLinksWithoutLanguage, row['universe_id'], row['parent_cat_id'], row['category_id'], row['sub_category_id'])
                full_df = pd.concat([full_df, temp_df])
                full_df.to_csv(f"../data/vc_tests/{catalogLinksWithoutLanguage.replace('/','')}.csv")
                print(f"Collecting page {i+1}: {row['universe']}, {row['parent_cat']}, {row['category']}, {row['sub_category']}.")
        print(f"Finished Collecting: {row['universe']}, {row['parent_cat']}, {row['category']}, {row['sub_category']}.\n")
    if continuous == True:
        full_df.to_csv(f"../data/vc_continuous/{catalogLinksWithoutLanguage.replace('/','')}/{datetime.today().date()}.csv")
    else: 
        full_df.to_csv(f"../data/{catalogLinksWithoutLanguage.replace('/','')}_full_vc.csv")
    return full_df

    
if __name__ == '__main__': 
    # Test dynamic headers first
    test_dynamic_headers()
    
    #vestiaire_scraper('5439','/balzac-paris/')
    brand_id = 308
    catalogLinksWithoutLanguage = '/bash/'
    brand_id = 15
    catalogLinksWithoutLanguage = '/isabel-marant/'
    brand_id = 5439
    catalogLinksWithoutLanguage = '/balzac-paris/'
    brand_id = 229
    catalogLinksWithoutLanguage = '/canada-goose/' 
    brand_id = 28
    catalogLinksWithoutLanguage = '/zadig-voltaire/'
    full_cat_vc_api_call(brand_id, catalogLinksWithoutLanguage, True, False)
    
    '''We're gonna do a manual split
    by macro_category: 
    parent cat (accessoires, chaussures, sacs), category within vetement(each individually)
    '''