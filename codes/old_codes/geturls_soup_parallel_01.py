from bs4 import BeautifulSoup
import requests
import aiohttp
import asyncio
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

def insertURLS(processed_links, export_file_path, id):
    # export the processed links to a CSV
    with open(export_file_path, 'a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        for link in processed_links:
            writer.writerow([id, link])

def cleanURLS(export_file_path, base_url):
    df = pd.read_csv(export_file_path, header=None)

    # Drop the duplicates
    df.drop_duplicates(subset=[1], inplace=True)  # Assuming the second column has index 1

    # Drop rows where the second column does not contain the base URL
    df = df[df[1].str.contains(base_url, na=False)]

    # Drop rows where the length of the second column is shorter than 10 characters
    df = df[df[1].str.len() >= 10]

    # Sort rows based on the length of the second column in ascending order
    df['length'] = df[1].str.len()
    df.sort_values(by='length', ascending=True, inplace=True)
    df.drop(columns=['length'], inplace=True)  # Drop the temporary length column

    # Write the cleaned DataFrame back to the CSV file
    df.to_csv(export_file_path, index=False, header=None)

    
async def main(import_csv_name, export_csv_name, site_name, base_url):
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
    async with aiohttp.ClientSession() as session:
        for id, site, status in zip(site_id, site_list, site_status):
            # check if the row has been processed
            if status == 'yes':
                continue

            try:
                # add timeout timer 10-15s
                # request with beautifulsoup and scraperapi
                url = site
                if 'http' in url[:5]:
                    async with session.get(url, proxy = proxies['http']) as response:
                        soup = BeautifulSoup(await response.text(), 'html.parser')
                else:
                    async with session.get(url, proxy = proxies['https']) as response:
                        soup = BeautifulSoup(await response.text(), 'html.parser')

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

                # create log list tags for the site including the id, scraping time
                # cleaning time, url, success or not, if not success, scraping time as MAX, cleaning time as NA
                # print the id of the site
                # take time of the scraping
                print(f"Finishing scraping for: {site} takes {timer():.2f}")

                updateCSV(import_file_path, id, 'yes')
                

                # clean the urls
                # take time of cleaning the urls
                insertURLS(processed_links, export_file_path, id)
                cleanURLS(export_file_path, base_url)
                print(f"Cleaning urls for {site} takes {timer():.2f}")


            except Exception as e:
                print(e)
                print(f"Fail to get urls for {site}")
                updateCSV(import_file_path, id, 'fail')
                continue

async def activator(import_csv_name, export_csv_name, site_name, base_url):
    await main(import_csv_name, export_csv_name, site_name, base_url)

def getURLS(import_csv_name, export_csv_name, site_name, base_url):
    asyncio.run(activator(import_csv_name, export_csv_name, site_name, base_url))
    
# getURLS("urls-wayback.csv", "urls_uncleaned.csv", "foxnews", sites["foxnews"]['base_url'])
# updateCSV("data/foxnews/urls-wayback.csv", 20230101110821, 'yes')
