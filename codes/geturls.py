from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from seleniumwire import webdriver

import undetected_chromedriver as uc
import csv
import time
import pandas as pd
import credentials
import json

config = credentials.get()
API_KEY = config['SCRAPERAPI_KEY']

proxy_options = {
  'proxy': {
    "http": f"scraperapi:{API_KEY}@proxy-server.scraperapi.com:8001",
    "https": f"scraperapi:{API_KEY}@proxy-server.scraperapi.com:8001"
  }
}

with open('sites.json', 'r') as f:
        sites = json.load(f)


def createDriver():
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox") # Bypass OS security model
    chrome_options.add_argument("--disable-dev-shm-usage") # This flag is used to disable the use of the /dev/shm shared memory file system in Chrome.
    # chrome_options.add_argument("--disable-gpu")
    # chrome_options.add_argument("--windox-size=800,600")
    service = Service(executable_path="../chromedriver-mac-arm64/chromedriver")

    driver = uc.Chrome(service=service, options=chrome_options, seleniumwire_options=proxy_options)
    return driver

def cleanURLS(processed_links, export_csv_name, site_name, base_url):
    # export the processed links to a CSV
    with open('data/'+site_name+'/'+export_csv_name, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        for link in processed_links:
            writer.writerow([link])

    # drop the duplicates 
    df = pd.read_csv('data/'+site_name+'/'+export_csv_name, header=None)
    df.drop_duplicates(subset=0, inplace=True)
    # drop the rows that contain less than 10 characters
    df = df[df[0].str.len() > 10]
    # if row does not contain base url, drop the row
    df = df[df[0].str.contains(base_url)]
    # sort the row by length in ascending order
    df['length'] = df[0].str.len()
    df = df.sort_values(by='length', ascending=True)
    # drop the temporary column
    df = df.drop(columns=['length'])
    
    df.to_csv('data/'+site_name+'/'+export_csv_name, index=False, header=None)

    
def getURLS(file_path, export_csv_name, site_name, base_url):
    print("Getting URLs for:", site_name)
    # import the website link from a CSV called urls.csv
    site_list = []
    site_index = []
    site_status = []
    with open('data/'+site_name+'/'+file_path, 'r', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file)
        # next(csv_reader) # Skip the header if there is one
        for row in csv_reader:
            site_list.append(row[1])
            site_index.append(row[0])
            site_status.append(row[2])

    # Remove wayback machine links from the urls
    processed_links = []  
    for site in site_list:
        # check if the row has been processed
        if site_status[site_list.index(site)] == 'yes':
            continue

        start_time = time.time()
        try:
            driver = createDriver()
            driver.get(site)

            # find all <a> elements and extract the hrefs
            articles  = WebDriverWait(driver, 30).until(
                EC.presence_of_all_elements_located((
                    By.CSS_SELECTOR,
                    'a'
                ))
            )
            links = [article.get_attribute('href') for article in articles]
        
            for link in links:
                if link is None:
                    continue
                # Locate the index that starts after 5 and that contains 'https' 
                new_link = link[5:]
                index_start = new_link.find('http')
                processed_link = link[index_start:]
                processed_links.append(processed_link)
                print(processed_link)

            # Close the driver after you're done
            driver.quit()
            end_time = time.time()

            # print the index of the site
            print("Finishing scraping for:", site_index[site_list.index(site)])
            print("Total time taken for {0} : {1}s".format(site_index[site_list.index(site)], (end_time - start_time)))

            # mark the row as processed
            df = pd.read_csv('data/' + site_name + '/' + file_path, header=None, encoding='utf-8')

            # Find the row and modify the third column to 'yes'
            identifier = int(site_index[site_list.index(site)])
            df.loc[df[0] == identifier, 2] = 'yes'
            
            # Write the modified DataFrame back to the CSV file
            df.to_csv('data/' + site_name + '/' + file_path, index=False, header=None, encoding='utf-8')

            # clean the urls
            start_time = time.time()
            cleanURLS(processed_links, export_csv_name, site_name, base_url)
            end_time = time.time()
            print("Total time taken for {0} : {1}s".format("cleanURLS", (end_time - start_time)))    

            # pause 10 seconds
            time.sleep(10)

        except Exception as e:
            print("Error: ", e)
            # mark the row as failed
            df = pd.read_csv('data/' + site_name + '/' + file_path, header=None, encoding='utf-8')

            # Find the row and modify the third column to 'yes'
            identifier = int(site_index[site_list.index(site)])
            df.loc[df[0] == identifier, 2] = 'fail'
            
            # Write the modified DataFrame back to the CSV file
            df.to_csv('data/' + site_name + '/' + file_path, index=False, header=None, encoding='utf-8')


# for i in range(9,10):
#     i = str(i)
#     getURLS("urls-wayback.csv", "urls_uncleaned.csv", sites[i]['name'], sites[i]['base_url'])

'''
approach one didn't work since most sites have frequent updates and changes
need to figure out a general approach to get the urls
'''