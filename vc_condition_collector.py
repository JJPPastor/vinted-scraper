
import pandas as pd
from selenium import webdriver
from bs4 import BeautifulSoup
from urllib import request
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.service import Service
import random
from selenium.common.exceptions import NoSuchElementException, TimeoutException, ElementNotInteractableException, ElementClickInterceptedException, WebDriverException
from selenium.webdriver.common.keys import Keys
from  datetime import datetime
import progressbar
from datetime import datetime
import timeit
import asyncio
import numpy as np
import re
import psutil
from urllib3.exceptions import MaxRetryError
import requests

import os
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound, Conflict

def driver_setup():
    try:
        s = Service("/Users/julespastor/Desktop/chromedriver")
        options = Options()
        options.binary_location = "/Applications/Google Chrome Beta.app/Contents/MacOS/Google Chrome Beta"
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--window-size=2880,1800")
        user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 Safari/537.36'
        options.add_argument(f'user-agent={user_agent}')
        driver = webdriver.Chrome(service=s, options=options)
    except :
        try:
            s = Service('/usr/local/bin/chromedriver')
            options = Options()
            options.binary_location = '/usr/bin/chrome-linux64/chrome'
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 Safari/537.36'
            options.add_argument(f'user-agent={user_agent}')
            options.add_argument("--window-size=1920,1080")

            # Initialize Chrome WebDriver using Service and ChromeOptions
            driver = webdriver.Chrome(service=s, options=options)
        except:
            s = Service("/Users/julespastor/Desktop/chromedriver")
            options = Options()
            options.binary_location = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
            options.add_argument("--headless")
            options.add_argument("--no-sandbox")
            options.add_argument("--window-size=2880,1800")
            user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 Safari/537.36'
            options.add_argument(f'user-agent={user_agent}')
            driver = webdriver.Chrome(service=s, options=options)
    return driver



async def nunery():
    return None

async def get_page(driver, url):
    print(url)
    driver.get(url)
    html = driver.page_source
    return html


async def condition_getter(urls):
    conditions = []
    a_driver = driver_setup()
    b_driver = driver_setup()
    c_driver = driver_setup()
    
    splits = np.array_split(urls, 3)
    list_a = splits[0]
    #print(list_a)
    list_b = splits[1]
    #print(list_b)
    list_c = splits[2]
    #print(list_c)
    #list_d = splits[3]

    ha = []
    hb = []
    hc = []
    #hd = []


    for i in range(len(list_a)):
        task_a = asyncio.create_task(get_page(a_driver, list_a[i]))
        try:
            task_b = asyncio.create_task(get_page(b_driver, list_b[i]))
        except IndexError:
            task_b = asyncio.create_task(nunery())
        try:
            task_c = asyncio.create_task(get_page(c_driver, list_c[i]))
        except IndexError:
            task_c = asyncio.create_task(nunery())
        '''try:
            task_d = asyncio.create_task(get_page(d_driver, list_d[i], i))
        except IndexError:
            task_d = asyncio.create_task(nunery())'''


        try:
            a1 = await task_a
        except:
            pass

        try:
            b1 = await task_b
        except:
            pass

        try:
            c1 = await task_c
        except:
            pass


        ha.append(a1)
        hb.append(b1)
        hc.append(c1)
        #hd.append(d1)
        
    a_driver.close()
    b_driver.close()
    c_driver.close()

    #print(len(ha), len(hb), len(hc))

    all_pages = ha + hb + hc #+ hd
    
    for page in all_pages:
        conditions.append(parser(page))
        
        
    return conditions


def parser(html):
    try:
        #return listing_name, description, color, condition, size, location, availability, price, currency photo links
        soup = BeautifulSoup(html, 'html.parser')
        lis = soup.find_all('li', class_='product-description-list_descriptionList__listItem__lTdIL')
        for li in lis: 
            if 'état' in li.text.lower():
                if 'quette' in li.text.lower(): 
                    print('new')
                    return 'état neuf'
                elif 'jamais' in li.text.lower():
                    print('excel')
                    return 'excellent état'
                elif 'très' in li.text.lower(): 
                    print('tb')
                    return 'très bon état'
                elif 'bon' in li.text.lower(): 
                    print('g')
                    return 'bon état'
                elif 'correct' in li.text.lower(): 
                    print('g')
                    return 'bon état'
                else:
                    return 'unmapped'
            else:
                continue
    except: 
        pass
        
        
async def macro_cond_collector(path): 
    df = pd.read_csv(path)
    df['link'] = 'https://www.vestiairecollective.com' + df['link']
    urls = df['link'].to_list()
    print(len(urls))
    conditions = await condition_getter(urls)
    if len(conditions) != len(urls): 
        conditions.pop()
    df['condition'] = conditions
    print(df)
    df.to_csv('../data/vc_tests/cond_test.csv')
    
    
if __name__ == '__main__': 
    asyncio.run(macro_cond_collector('../data/balzac-paris_full_vc.csv'))
    #driver = driver_setup()
    #html = asyncio.run(get_page(driver, 'https://www.vestiairecollective.com/women-clothing/coats/other-stories/black-wool-other-stories-coat-57334896.shtml'))
    #print(html)