from newsplease import NewsPlease
import pandas as pd
import time
from threading import Thread
import json
import csv

class ArticleFetcher(Thread):
    def __init__(self, urls):
        Thread.__init__(self)
        self.urls = urls
        self.articles = []

    def run(self):
        for url in self.urls:
            try:
                article = NewsPlease.from_url(url, timeout=10)  # Adjusted timeout
                self.articles.append((url, article.get_serializable_dict() if article else None))
            except Exception as e:
                print(f"Error fetching article for URL {url}: {e}")
                self.articles.append((url, None))

def fetch_articles_in_threads(file_path, num_threads=5):
    # Read URLs from CSV
    urls_list = []
    with open(file_path, 'r', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader)  # Skip header
        for row in csv_reader:
            urls_list.append(row[0])
    
    # Split URLs among threads
    chunk_size = len(urls_list) // num_threads
    threads = []
    for i in range(num_threads):
        start = i * chunk_size
        # Ensure the last thread picks up any remaining URLs
        end = None if i+1 == num_threads else start + chunk_size
        thread = ArticleFetcher(urls_list[start:end])
        threads.append(thread)
        thread.start()
    
    # Wait for all threads to finish
    for thread in threads:
        thread.join()
    
    # Collect articles from all threads
    all_articles = {}
    for thread in threads:
        for url, article in thread.articles:
            all_articles[url] = article

    # Write to JSON
    json_file_path = "data/bbc/articles.json"
    with open(json_file_path, "w", encoding='utf-8') as file:
        json.dump(all_articles, file, ensure_ascii=False, indent=4)

# Example usage
fetch_articles_in_threads('data/bbc/urls_cleaned.csv', num_threads=5)
