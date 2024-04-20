
from newsplease import NewsPlease
import pandas as pd
import time
from threading import Thread
import json
import csv

site_list = ["bbc", "cnn", "foxnews", "nationalreview", "nytimes"]    

class ArticleFetcher(Thread):
    def __init__(self, site_data, site):
        Thread.__init__(self)
        self.site_data = site_data
        self.site = site
        self.articles = []

    def run(self):
        for row in self.site_data:
            site_id, url, status = row
            # print site_id, url, status
            print(f"site_name: {self.site}; site_id: {site_id}")
            try:
                article = NewsPlease.from_url(url, timeout=10)  # Adjusted timeout
                if article:
                    article_dict = article.get_serializable_dict()
                    # add a new key to the dictionary
                    article_dict["wayback_id"] = site_id
                    self.articles.append((site_id, url, article_dict, 'yes'))
                else:
                    self.articles.append((site_id, url, None, 'none'))
            except Exception as e:
                print(f"Error fetching article for URL {url}: {e}")
                self.articles.append((site_id, url, None, 'fail'))

def fetch_articles_in_threads(site, num_threads=5):
    # Read URLs from CSV
    site_data = []
    with open("data/"+site+"/urls_cleaned.csv", 'r', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file)
        for row in csv_reader:
            if row[2] == "yes":
                continue
            site_data.append(row)
    
    # Split URLs among threads
    chunk_size = len(site_data) // num_threads + 1
    threads = []
    for i in range(num_threads):
        start = i * chunk_size
        end = None if i+1 == num_threads else start + chunk_size
        thread = ArticleFetcher(site_data[start:end], site)
        threads.append(thread)
        thread.start()
    
    # Wait for all threads to finish
    for thread in threads:
        thread.join()

    # all_articles = {}
    # Collect articles from all threads and update CSV
    df = pd.read_csv("data/"+site+"/urls_cleaned.csv", header=None, encoding='utf-8')
    for thread in threads:
        all_articles = {}
        for site_id, url, article, status in thread.articles:
            df.loc[df[1] == url, 2] = status
            all_articles[url] = article
            # Write article to JSON if successfully fetched
            # if status == 'yes':
            #     json_file_path = f"data/{site}/articles_new.json"
            #     with open(json_file_path, "a", encoding='utf-8') as file:
            #         json.dump(all_articles, file, ensure_ascii=False, indent=4)
        
        json_file_path = f"data/{site}/articles_new.json"

        try:
            with open(json_file_path, "r", encoding='utf-8') as file:
                existing_data = json.load(file)
        except FileNotFoundError:
            existing_data = {}

        # Update the existing data with the new articles
        existing_data.update(all_articles)

        # Write the updated data back to the file
        with open(json_file_path, "w", encoding='utf-8') as file:
            json.dump(existing_data, file, ensure_ascii=False, indent=4)


    # Update the CSV with the status of article retrieval
    df.to_csv("data/"+site+"/urls_cleaned.csv", header=False, index=False, encoding='utf-8')

# # Example usage
# fetch_articles_in_threads("cnn", num_threads=5)
