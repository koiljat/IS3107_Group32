from bs4 import BeautifulSoup as bs
from time import sleep
import time
import numpy as np
import pandas as pd
import requests
import re
from datetime import datetime, timedelta

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
    dates = []
    domain = "https://www.sgcarmart.com/used_cars/"
    for item in soup.findAll('strong'):
        try:
            link = item.find('a')
            if 'info' in link['href']:
                links.append(domain + link['href'])
        except:
            continue
    for item in soup.findAll(class_='font_gray_light font_10'):
        try:
            date = item.text.strip()
            date_object = datetime.strptime(date, "Posted: %d-%b-%Y")
            formatted_date_string = date_object.strftime("%Y-%m-%d")
            dates.append(formatted_date_string)
        except:
            continue
    return links, dates

def is_more_than_one_day_ago(given_date):
    # Convert the given date string to a datetime object
    given_date_obj = datetime.strptime(given_date, "%Y-%m-%d")
    
    # Get today's date
    today_date = datetime.today().date()
    
    # Calculate the difference between today's date and the given date
    difference = today_date - given_date_obj.date()
    
    # Check if the difference is more than 1 day
    return difference.days > 1

def get_daily_links(soup):
    links = []
    dates = []
    domain = "https://www.sgcarmart.com/used_cars/"
    total_links = soup.findAll('strong')
    total_dates = soup.findAll(class_='font_gray_light font_10')
    for i in range(len(total_dates)):
        item = total_links[i]
        date = total_dates[i]
        try:
            date = date.text.strip()
            date_object = datetime.strptime(date, "Posted: %d-%b-%Y")
            formatted_date_string = date_object.strftime("%Y-%m-%d")
            if is_more_than_one_day_ago(formatted_date_string):
                return links, dates
            dates.append(formatted_date_string)
            link = item.find('a')
            if 'info' in link['href']:
                links.append(domain + link['href'])
        except:
            continue
    return links, dates



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
    links = get_links(soup)[0]
    dates = get_links(soup)[1]
    info = get_info(links)
    end_time = time.time()
    time_elapsed = end_time - start_time
    print(f"Page {page_num} completed in {time_elapsed/60}m")
    print("--------------------------")
    col_names = ["name","price","depreciation","mileage","eng_cap","power","reg_date","coe_left","owners","omv","arf","accessories"]
    df = pd.DataFrame(info, columns=col_names)
    df["date_listed"] = dates
    
    return df

'''
Scrapes all listings
returns a pandas dataframe
'''
def run_scraper():
    max_page = 15
    df = run_test_scraper(1)
    for i in range(2, max_page+1):
        new_df = run_test_scraper(i)
        df = pd.concat([df, new_df])
    return df
        
def run_daily_scraper():
    data = []
    start_time = time.time()
    all_dates = []
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
        links_dates = get_daily_links(soup)
        links = links_dates[0]
        dates = links_dates[1]
        info = get_info(links)
        data.extend(info)
        all_dates.extend(dates)
        end_time = time.time()
        if any(is_more_than_one_day_ago(date) for date in dates):
            break
        print(f"Page {page//100+1} completed")
        print("--------------------------")
        if (page == tp):
            time_elapsed = end_time - start_time
            col_names = ["name","price","depreciation","mileage","eng_cap","power","reg_date","coe_left","owners","omv","arf","accessories"]
            df = pd.DataFrame(data, columns=col_names)
            df["date_listed"] = all_dates
            print(f"Scraping completed in {time_elapsed/60}m.")

            return df

        sleep(2)
    col_names = ["name","price","depreciation","mileage","eng_cap","power","reg_date","coe_left","owners","omv","arf","accessories"]
    df = pd.DataFrame(data, columns=col_names)
    dates = dates[:len(df)]
    df["date_listed"] = all_dates
    
    return df

'''
Transform and clean the sgcarmart csv in staging before loading into data warehouse
Input is the sgcarmart dataframe
Output is the cleaned dataframe
'''
def transform_sgcarmart_data(df):
    # remove any rows with null values
    df_cleaned = df.replace("NA", np.nan)
    df_cleaned = df_cleaned.replace("", np.nan)
    df_cleaned = df_cleaned.dropna(how="any")
    df_cleaned[df_cleaned.isin([np.nan]).any(axis=1)]

    # change data types
    num_cols = ["price", "depreciation", "mileage", "owners", "omv", "arf", "eng_cap", "power"]
    df_cleaned["owners"] = df_cleaned["owners"].str.replace("More than 6", "6")
    for col in num_cols:
        df_cleaned[col] = df_cleaned[col].astype("string").str.replace(",","").astype("float")

    # remove parenthesis in car name
    df_cleaned["name"] = df_cleaned["name"].str.replace(r'\(([^)]+)\)', "", regex=True)

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


