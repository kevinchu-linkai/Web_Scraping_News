from bs4 import BeautifulSoup
import requests

import csv
import time
import pandas as pd
import credentials
import json

config = credentials.get()
API_KEY = config['SCRAPERAPI_KEY']
current_time = time.time()

# proxy_options = {
#   'proxy': {
#     "http": f"scraperapi:{API_KEY}@proxy-server.scraperapi.com:8001",
#     "https": f"scraperapi:{API_KEY}@proxy-server.scraperapi.com:8001"
#   }
# }

proxies = {
    "http": f"http://scraperapi:{API_KEY}@proxy-server.scraperapi.com:8001",
    "https": f"https://scraperapi:{API_KEY}@proxy-server.scraperapi.com:8001"
}

with open('sites.json', 'r') as f:
        sites = json.load(f)

def timer():
    global current_time
    temp_time = current_time
    end_time = time.time()
    current_time = end_time
    return end_time - temp_time

def updateCSV(import_file_path, id, status):
    print("Updating CSV file")
    print(import_file_path, id, status)
    # mark the row as processed
    df = pd.read_csv(import_file_path, header=None, encoding='utf-8')

    # find the row with id and modify the third column to status
    df.loc[df[0] == id, 2] = status

    # Write the modified DataFrame back to the CSV file
    df.to_csv(import_file_path, index=False, header=None, encoding='utf-8')

def cleanURLS(processed_links, export_file_path, base_url):
    # export the processed links to a CSV
    with open(export_file_path, 'a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        for link in processed_links:
            writer.writerow([link])

    df = pd.read_csv(export_file_path, header=None)

    # drop the duplicates 
    df.drop_duplicates(subset=0, inplace=True)

    # if row does not contain base url, drop the row
    df = df[df[0].str.contains(base_url)]

    # sort the row by length in ascending order
    df['length'] = df[0].str.len()
    df = df.sort_values(by='length', ascending=True)

    # drop the temporary column
    df = df.drop(columns=['length'])
    
    df.to_csv(export_file_path, index=False, header=None)

    
def getURLS(import_csv_name, export_csv_name, site_name, base_url):
    print("Getting URLs for:", site_name)

    import_file_path = 'data/' + site_name + '/' + import_csv_name
    export_file_path = 'data/' + site_name + '/' + export_csv_name

    # import the website link from a CSV called urls.csv
    site_list = []
    site_id = []
    site_status = []
    with open(import_file_path, 'r', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file)
        # next(csv_reader) # Skip the header if there is one
        for row in csv_reader:
            site_id.append(int(row[0])) # id is type int
            site_list.append(row[1])
            site_status.append(row[2])

    # Remove wayback machine links from the urls
    processed_links = []  
    for id, site, status in zip(site_id, site_list, site_status):
        # check if the row has been processed
        if status == 'yes':
            continue

        try:
            # request with beautifulsoup and scraperapi
            url = site
            # response = requests.get(url, proxies=proxy_options['proxy']) # might need to specify the proxy option
            response = requests.get(url, proxies=proxies)
            soup = BeautifulSoup(response.text, 'html.parser')

            # find all the urls
            links = [a['href'] for a in soup.find_all('a', href=True)]
        
            for link in links:
                if link is None or len(link) < 10:
                    continue

                try:
                    # Locate the id that starts after 5 and that contains 'http' 
                    new_link = link[5:]
                    id_start = new_link.find('http')
                    processed_link = new_link[id_start:]
                    processed_links.append(processed_link)
                    print(processed_link)

                except:
                    print("Website url does not contain 'http': ", link)
                    continue

            # print the id of the site
            print(f"Finishing scraping for: {site} takes {timer():.2f}")

            updateCSV(import_file_path, id, 'yes')
            

            # clean the urls
            cleanURLS(processed_links, export_file_path, base_url)
            print(f"Cleaning urls for {site} takes {timer():.2f}")


        except Exception as e:
            print(e)
            print(f"Fail to get urls for {site}")
            updateCSV(import_file_path, id, 'fail')
            continue

# getURLS("urls-wayback.csv", "urls_uncleaned.csv", "foxnews", sites["foxnews"]['base_url'])
# updateCSV("data/foxnews/urls-wayback.csv", 20230101110821, 'yes')
