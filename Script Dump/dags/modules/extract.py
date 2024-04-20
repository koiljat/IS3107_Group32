import requests
from bs4 import BeautifulSoup
import logging
import pandas as pd
from datetime import datetime

logging.basicConfig(level=logging.INFO)

def get_links():
    """
    Retrieves a list of links to used cars from a website.

    Returns:
        list: A list of links to used cars.
    """
    page_number = 1
    res = []

    while True:
        url = f"https://www.motorist.sg/used-cars?page={page_number}&view=grid&keywords=&price_min=&price_max=&depre_min=&depre_max=&year_min=&year_max=&mileage_min=&mileage_max=&coe_min=&coe_max=&ownership=&transmission=&fuel=&dealers_undefined=&vehicles=&availability=available"

        response = requests.get(url)
        if response.status_code == 200:
            logging.info(f"Loaded page {page_number}")
            soup = BeautifulSoup(response.text, 'html.parser')
            
            links = soup.find_all('a', class_='text-decoration-none w-100 h-100 btn-open-webview')
            links = [link['href'] for link in links]
            
            res.extend(links)
            if page_number >= 5:
                break
            
                
            if 'id="next_link"' not in response.text:
                logging.info(f"Last page reached")
                break
            else:
                page_number += 1
        else:
            logging.info(f"Failed to load page {page_number}")
    return res

def get_details(x):
    """
    Scrapes car details from a list of URLs and returns the details in a pandas DataFrame.

    Returns:
        pandas.DataFrame: A DataFrame containing the scraped car details.
    """    
    res = pd.DataFrame(columns=["model", "price", "depreciation", "registration_date", 
                                "no_of_owner", "milleage", "omv", "arf", "power", "capacity", 
                                "features", "coe_expiry_date", "coe_left" "accessories", "description, date"])
    count = 1
    total = len(x)
    for url in x:
        try:
            response = requests.get(url)
        except:
            logging.info(f"Failed to load page {url}")
            break
        logging.info(f"Loading page {count} / {total}")
        count += 1
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            
            model = soup.find('h1', class_="used-car-listing-header vehicle-listing-header header mb-2").text.strip()
            costs = soup.find('div', class_='d-flex align-items-center').text.strip().split()
            price = costs[0]
            depreciation = costs[1]
            
            info_list = soup.find('div', class_='bg-light-grey rounded-10 px-4 pt-4 pb-3 my-4').text.strip().split()
            
            try:
                registration_date = info_list[info_list.index('Registration')+2]
                no_of_owner = info_list[info_list.index('Ownership')+1]
                milleage = info_list[info_list.index('Mileage')+1]
                omv = info_list[info_list.index('OMV')+1]
                arf = info_list[info_list.index('ARF')+1]
                power = info_list[info_list.index('Power')+1]
                capacity = info_list[info_list.index('Capacity')+1]
                coe = info_list[info_list.index('More')+4] ##Getting COE, nothign wrong here!
                coe_left = soup.find_all("span", class_="d-block font-12 mt-n1")[1].text
                seller_note = soup.find_all("div", class_="mb-4")
                seller_note = [x.text.strip() for x in seller_note]
                features = seller_note[0].strip()
                accessories = seller_note[1].strip()
                description = seller_note[2].strip()
            except:
                continue
            
            date = soup.find(class_='d-flex align-items-center text-grey font-14').text.strip()
            date_object = datetime.strptime(date, "Posted on %d %b %Y")

            formatted_date_string = date_object.strftime("%Y-%m-%d")
            
            
            
            curr_data = pd.DataFrame({"model": model, 
                                      "price": price, 
                                      "depreciation": depreciation, 
                                      "registration_date": registration_date, 
                                      "no_of_owner": no_of_owner, 
                                      "milleage": milleage, 
                                      "omv": omv, 
                                      "arf": arf, 
                                      "power": power,
                                      "capacity": capacity,
                                      "features": features, 
                                      "coe_expiry_date": coe, 
                                      "coe_left": coe_left,
                                      "accessories": accessories, 
                                      "description": description,
                                      "date": formatted_date_string},
                                      index=[0])
            
            res = pd.concat([res, curr_data], ignore_index=True)
        else:
            logging.info(f"Failed to load page {url}")
            break
    return res
