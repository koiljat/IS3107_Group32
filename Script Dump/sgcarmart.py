from bs4 import BeautifulSoup as bs
from time import sleep
import time
import numpy as np
import pandas as pd
import requests
import re
import csv

'''
Find the total number of listings
'''
def total_pages(soup):
    element = soup.find("p", class_="vehiclenum").text.strip().split(" ")[0]
    num_of_listings = int(element.replace(",",""))
    num_of_pages = num_of_listings // 100 + 1
    return num_of_pages

'''
Get listing links for one page
'''
def get_links(soup):
    links = []
    domain = "https://www.sgcarmart.com/used_cars/"
    for item in soup.findAll('strong'):
        try:
            link = item.find('a')
            if 'info' in link['href']:
                links.append(domain + link['href'])
        except:
            continue
    return links

'''
Get all the features of a listing
'''
def get_name(soup):
    try:
        name = soup.find('div',{'id':'toMap'}).text.strip()
    except:
         name = 'NA'
    return name

def get_price(soup):
    try:
        price = soup.find('td',{'class':'font_red'}).text.strip()[1:]
    except:
        price = 'NA'
    return price

def get_depre(soup):
    try:
        depre = re.findall(r'\d+,\d+',soup.findAll('tr',{'class':'row_bg'})[1].find('td',{'class':None}).text)[0]
    except:
        depre = 'NA'
    return depre

def get_miles(soup):
    try:
        mileage = re.findall(r'\d+,\d+',soup.find('div',{'class':'row_info'}).text.strip())[0]
    except:
        mileage = 'NA'
    return mileage

def get_engcap(soup):
    try:
        engine_cap = re.findall(r'\d+,*\d+',soup.findAll('div',{'class':'row_info'})[4].text)[0]
    except:
        engine_cap = 'NA'
    return engine_cap

def get_regdate(soup):
    try:
        reg_date = re.findall(r'\d{2}-\w{3}-\d{4}',soup.findAll('tr',{'class':'row_bg'})[1].findAll('td',{'class':'rightColData'})[-1].text)[0]
    except:
        reg_date = 'NA'
    return reg_date

def get_power(soup):
    try:
        power = re.findall(r'\d+\.\d+',soup.findAll('div',{'class':'row_info'})[-2].text)[0]
    except:
        power = 'NA'
    return power

def get_owners(soup):
    try:
        owners = soup.findAll('div',{'class':'row_info'})[-1].text.strip()
    except:
        owners = 'NA'
    return owners

def get_omv(soup):
    try:
        omv = "NA"
        info = soup.findAll('div',{'class':'eachInfo'})
        for row in info:
            if row.find('div', {'class':'row_title'}).text.strip() == "OMV":
                omv = re.findall(r'\d+,\d+',row.find('div', {'class':'row_info'}).text.strip())[0]
    except:
        omv = "NA"
    return omv

def get_arf(soup):
    try:
        arf = "NA"
        info = soup.findAll('div',{'class':'eachInfo'})
        for row in info:
            if row.find('div', {'class':'row_title'}).text.strip() == "ARF":
                arf = re.findall(r'\d+,\d+',row.find('div', {'class':'row_info'}).text.strip())[0]
    except:
        arf = "NA"
    return arf

def get_accessories(soup):
    try:
        acc = re.findall(r'\n(.*)',soup.findAll('tr',{'class':'row_bg1'})[2].find('td').text)[1].strip()
    except:
        acc = "NA"
    return acc

def get_coe_left(soup):
    try:
        coe = re.findall(r"\((.*?)\)",soup.findAll('tr',{'class':'row_bg'})[1].findAll('td',{'class':'rightColData'})[-1].text)[0][:-9]
    except:
        coe = "NA"
    return coe

'''
Access each link and store the info in a list
'''
def get_info(links):
    info = []
    for link in links:
        html = requests.get(link)
        soup = bs(html.text,'lxml')
        
        name = get_name(soup)
        price = get_price(soup)
        depre = get_depre(soup)
        mileage = get_miles(soup)
        eng_cap = get_engcap(soup)
        reg_date = get_regdate(soup)
        coe_left = get_coe_left(soup)
        power = get_power(soup)
        owners = get_owners(soup)
        omv = get_omv(soup)
        arf = get_arf(soup)
        accessories = get_accessories(soup)
        
        info.append([name,price,depre,mileage,eng_cap,power,reg_date,coe_left,owners,omv,arf,accessories])
    
    return info

'''
Test scraper that only scrapes one page --> page_num
returns a panda dataframe
'''
def run_test_scraper(page_num):
    start_time = time.time()
    # Set 100 listings in one page
    url = "https://www.sgcarmart.com/used_cars/listing.php?BRSR={}&RPG=100&AVL=2&VEH=0"
    print(f"Test scraper running...")
    print("--------------------------")
    html = requests.get(url.format((page_num-1)*100))
    soup = bs(html.text,'lxml')
    links = get_links(soup)
    info = get_info(links)
    end_time = time.time()
    time_elapsed = end_time - start_time
    print(f"Page {page_num} completed in {time_elapsed/60}m")
    print("--------------------------")
    col_names = ["name","price","depreciation","mileage","eng_cap","power","reg_date","coe_left","owners","omv","arf","accessories"]
    df = pd.DataFrame(info, columns=col_names)
    
    return df

'''
Scrapes all listings
returns a pandas dataframe
'''
def run_scraper():
    data = []
    start_time = time.time()
    # Set 100 listings in one page
    url = "https://www.sgcarmart.com/used_cars/listing.php?BRSR={}&RPG=100&AVL=2&VEH=0"
    html = requests.get(url.format(0))
    soup = bs(html.text,'lxml')
    tp = total_pages(soup)
    print(f"Total number of pages is {tp}")
    print(f"Scraping in progress...")
    print("--------------------------")
    for page in range(0,tp*100,100):
        html = requests.get(url.format(page))
        soup = bs(html.text,'lxml')
        links = get_links(soup)
        info = get_info(links)
        data.extend(info)
        print(f"Page {page//100+1} completed")
        print("--------------------------")
        if (page == tp):
            time_elapsed = end_time - start_time
            col_names = ["name","price","depreciation","mileage","eng_cap","power","reg_date","coe_left","owners","omv","arf","accessories"]
            df = pd.DataFrame(data, columns=col_names)
            print(f"Scraping completed in {time_elapsed/60}m.")

            return df

        sleep(2)

'''
Transform and clean the sgcarmart csv in staging before loading into data warehouse
Input is the sgcarmart dataframe
Output is the cleaned dataframe
'''
def transform_sgcarmart(df):
    # remove any rows with null values
    df_cleaned = df.dropna(how="any")
    df_cleaned[df_cleaned.isin([np.nan]).any(axis=1)]

    # change data types
    num_cols = ["price", "depreciation", "mileage", "owners", "omv", "arf", "eng_cap", "power"]
    df_cleaned["owners"] = df_cleaned["owners"].replace("More than 6", "6")
    for col in num_cols:
        df_cleaned[col] = df_cleaned[col].replace(",","").astype("int")

    # remove parenthesis in car name
    df_cleaned["name"] = df_cleaned["name"].str.replace(r'\(([^)]+)\)', "", regex=True)
    df_cleaned.head()

    # convert coe_left to years
    def convert_to_years(x):
        
        years = re.search(r'(\d+)yrs', x)
        if years == None:
            years = 0
        else:
            years = int(years.group(1))
                    
        months = re.search(r'(\d+)mths', x)
        if months == None:
            months = 0
        else:
            months = int(months.group(1))
                    
        days = re.search(r'(\d+)days', x)
        if days == None:
            days = 0
        else:
            days = int(days.group(1))
        
        total_years = years + months/12 + days/365
        return round(total_years,2)

    df_cleaned["coe_left"] = df_cleaned["coe_left"].apply(convert_to_years)

    return df_cleaned


