import requests
from typing import Dict, List, Optional, Union
import json
import pandas as pd
import os
from time import sleep

def get_all_product_sheets(api_key: str, brand_name: str, limit: int = 100) -> List[Dict]:
    """
    Retrieve ALL product sheets from Meilisearch API for a given brand with pagination.
    
    Args:
        api_key (str): The API access token
        brand_name (str): The brand name to filter products
        limit (int): Number of documents per request (max 1000)
        
    Returns:
        List[Dict]: List of all product documents
        
    Raises:
        requests.RequestException: If the API request fails
        ValueError: If the response is invalid
    """
    
    # Brand mapping - you can extend this dictionary with more brands
    brand_mapping = {
        'balzac': {
            'id': 3,
            'domain': 'secondevie.balzac-paris.fr'
        }
        # Add more brands here as needed
        # 'other_brand': {'id': X, 'domain': 'domain.com'}
    }
    
    # Get brand info
    brand_info = brand_mapping.get(brand_name.lower())
    if not brand_info:
        raise ValueError(f"Brand '{brand_name}' not found. Available brands: {list(brand_mapping.keys())}")
    
    brand_id = brand_info['id']
    domain = brand_info['domain']
    
    # Headers
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json'
    }
    
    all_products = []
    offset = 0
    
    while True:
        # API endpoint with pagination
        url = f"https://search.faume.cloud/indexes/articles/documents?filter=brand%3D{brand_id}&limit={limit}&offset={offset}"
        
        try:
            # Make the API request
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            # Parse JSON response
            data = response.json()
            
            # Handle different response formats
            if isinstance(data, dict):
                if 'results' in data:
                    batch_products = data['results']
                    total = data.get('total', len(batch_products))
                elif 'hits' in data:
                    batch_products = data['hits']
                    total = data.get('estimatedTotalHits', len(batch_products))
                else:
                    batch_products = [data]
                    total = 1
            else:
                batch_products = data
                total = len(batch_products)
            
            # Add domain info to each product
            for product in batch_products:
                product['_domain'] = domain
            
            # Add to our collection
            all_products.extend(batch_products)
            
            # Check if we've got all products
            if len(batch_products) < limit:
                break
            
            # Update offset for next batch
            offset += limit
            
            # Add small delay to be respectful to the API
            sleep(0.1)
            
        except requests.RequestException as e:
            if len(all_products) == 0:
                raise requests.RequestException(f"API request failed: {str(e)}")
            else:
                break
        except json.JSONDecodeError as e:
            if len(all_products) == 0:
                raise ValueError(f"Invalid JSON response: {str(e)}")
            else:
                break
    return all_products

def construct_product_url(product: Dict, domain: str = None) -> str:
    """
    Construct the full product URL from a product document.
    
    Args:
        product (Dict): Product document from the API response
        domain (str, optional): Domain to use. If not provided, uses _domain from product
        
    Returns:
        str: Full product URL
    """
    if domain is None:
        domain = product.get('_domain', '')
    
    slug = product.get('slug', '')
    return f"https://{domain}/products/{slug}"

def get_article_urls(product: Dict, domain: str = None) -> List[str]:
    """
    Get all article URLs from a product's choices.
    
    Args:
        product (Dict): Product document from the API response
        domain (str, optional): Domain to use. If not provided, uses _domain from product
        
    Returns:
        List[str]: List of article URLs
    """
    if domain is None:
        domain = product.get('_domain', '')
    
    urls = []
    choices = product.get('choices', [])
    
    for choice in choices:
        deep_link = choice.get('@id', '')
        if deep_link:
            # The @id is already a path, so we just need to prepend the domain
            url = f"https://{domain}{deep_link}"
            urls.append(url)
    
    return urls

def extract_all_articles_from_products(products: List[Dict]) -> List[Dict]:
    """
    Extract all individual articles from product sheets.
    
    Args:
        products (List[Dict]): List of product documents
        
    Returns:
        List[Dict]: List of individual articles with product context
    """
    all_articles = []
    
    for product in products:
        # Get product-level information
        product_id = product.get('id', '')
        product_title = product.get('title', '')
        product_slug = product.get('slug', '')
        domain = product.get('_domain', '')
        
        # Extract individual articles from choices
        choices = product.get('choices', [])
        
        for choice in choices:
            article = {
                # Product-level info
                'product_id': product_id,
                'product_title': product_title,
                'product_slug': product_slug,
                'product_url': construct_product_url(product),
                'domain': domain,
                
                # Article-level info
                'article_id': choice.get('id', ''),
                'article_slug': choice.get('slug', ''),
                'article_title': choice.get('title', ''),
                'article_url': f"https://{domain}{choice.get('@id', '')}" if choice.get('@id') else '',
                
                # Pricing (convert from cents to euros)
                'price_eur': choice.get('price', 0) / 100 if choice.get('price') else 0,
                
                # Article attributes
                'state': choice.get('state', ''),
                'size': choice.get('size', ''),
                'type': choice.get('type', ''),
                'brand': choice.get('brand', ''),
                'color': choice.get('color', ''),
                'gender': choice.get('gender', ''),
                'season': choice.get('season', ''),
                'category': choice.get('category', ''),
                'sub_category': choice.get('sub_category', ''),
                'collection': choice.get('collection', ''),
                'color_image': choice.get('color_image', ''),
                'size_filters': choice.get('size_filters', ''),
                
                # Additional info
                'description': choice.get('description', ''),
                'information': choice.get('information', ''),
                'published_at': choice.get('publishedAt', ''),
                
                # Photos
                'photos_count': len(choice.get('photos', [])),
                'first_photo': choice.get('photos', [None])[0] if choice.get('photos') else None,
            }
            
            all_articles.append(article)
    
    return all_articles

def articles_to_dataframe(articles: List[Dict]) -> pd.DataFrame:
    """
    Convert articles list to a pandas DataFrame.
    
    Args:
        articles (List[Dict]): List of article dictionaries
        
    Returns:
        pd.DataFrame: DataFrame with article information
    """
    return pd.DataFrame(articles)

def create_price_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a price summary DataFrame grouped by sub_category with min, max, and average price.
    
    Args:
        df (pd.DataFrame): DataFrame with individual articles
        
    Returns:
        pd.DataFrame: Summary DataFrame with price statistics by sub_category
    """
    # Group by sub_category and calculate price statistics
    price_summary = df.groupby('sub_category').agg({
        'price_eur': ['min', 'max', 'mean', 'count']
    }).round(2)
    
    # Flatten column names
    price_summary.columns = ['min_price_eur', 'max_price_eur', 'avg_price_eur', 'product_count']
    
    # Reset index to make sub_category a column
    price_summary = price_summary.reset_index()
    
    return price_summary

def save_data_to_csv(api_key: str, brand_name: str, base_filename: str = None) -> Dict[str, str]:
    """
    Fetch all product data, extract articles, and save two CSV files:
    1. All products
    2. Price summary by sub_category
    
    Args:
        api_key (str): The API access token
        brand_name (str): The brand name to filter products
        base_filename (str, optional): Base filename prefix
        
    Returns:
        Dict[str, str]: Dictionary with CSV file paths
    """
    # Get all product data
    products = get_all_product_sheets(api_key, brand_name)
    
    # Extract all articles
    articles = extract_all_articles_from_products(products)
    
    # Convert to DataFrame
    df_articles = articles_to_dataframe(articles)
    
    # Create price summary
    df_price_summary = create_price_summary(df_articles)
    
    # Generate filenames
    if base_filename is None:
        base_filename = f"../data/faume_tests/{brand_name}_data"
    
    files_saved = {}
    
    # Save all articles
    articles_filename = f"{base_filename}_all_products.csv"
    df_articles.to_csv(articles_filename, index=False, encoding='utf-8')
    files_saved['all_products'] = articles_filename
    
    # Save price summary
    summary_filename = f"{base_filename}_price_summary.csv"
    df_price_summary.to_csv(summary_filename, index=False, encoding='utf-8')
    files_saved['price_summary'] = summary_filename
    
    return files_saved


def collection_analysis(): 
    df = pd.read_csv('../data/faume_tests/balzac_data_all_products.csv')
    prices_df = pd.read_csv('../data/faume_tests/prices_balzac.csv')
    prices_df = prices_df.rename(columns={'id':'article_id'})
    merged_df = pd.merge(df, prices_df, on='article_id', how='inner')
    merged_df[['price_resale', 'price_origin', 'price_offer']] /= 100
    merged_df = merged_df[['price_resale','price_origin','price_offer','type','state_x']]
    merged_df['state_x'] = merged_df['state_x'].str.lower()
    merged_df.to_csv('../data/faume_tests/balzac_test.csv')
    condition_order = ['état neuf', 'excellent état', 'très bon état', 'bon état']
    # Convert clean_condition to categorical with custom order before grouping
    merged_df['condition'] = pd.Categorical(merged_df['state_x'], categories=condition_order, ordered=True)     
    cond_df = merged_df[['price_resale','price_origin','price_offer','type','condition']].groupby(by=['type','condition']).agg(['mean','count']).round(2)
    #df_grouped = merged_df[['price_resale','price_origin','price_offer','type','condition']].groupby(by='collection').mean().round(2)
    cond_df = cond_df.reset_index(drop=False)
    cond_df.to_csv('../data/faume_tests/balzac_data_price_summary_alt.csv', index=False)

# Example usage
if __name__ == "__main__":
    # Replace with your actual API key
    '''API_KEY = "6579a1d771348689d2569a3957672b4fa6085dbe47cde5d403541f2fe83e919b"
    
    try:
        # Get all data and save to CSV files
        saved_files = save_data_to_csv(API_KEY, "balzac")
        
        print(f"Successfully created 2 CSV files:")
        print(f"1. All products: {saved_files['all_products']}")
        print(f"2. Price summary: {saved_files['price_summary']}")
        
        # Load the DataFrames to show what was created
        articles_df = pd.read_csv(saved_files['all_products'])
        summary_df = pd.read_csv(saved_files['price_summary'])
        
        print(f"\nAll products file contains {len(articles_df)} products")
        print(f"Price summary file contains {len(summary_df)} sub-categories")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()'''
        
    collection_analysis()