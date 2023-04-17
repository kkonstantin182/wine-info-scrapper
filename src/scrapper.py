import json
import requests
from bs4 import BeautifulSoup
import pandas as pd
from random import randint
import os

HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36"
    )
}
# COUNTRY = "Italy"
FIRST_PAGE = 1
BASE_URL = "https://www.winemag.com/?s=&drink_type=wine&country={country}&page={n_page}&sort_by=pub_date_web&sort_dir=desc&search_type=reviews"
DATA_DIR = "data"
FILE_NAME_URLS = "winemag-urls"
FILE_NAME_REVIEWS = "winemag-data"


class Scrapper:

    def __init__(self, country, n_pages):

        self.country = country
        self.n_pages = n_pages
        self.first_page = FIRST_PAGE
        self.init_url = BASE_URL.format(**{"country": self.country, "n_page": self.first_page})
        self.total_reviews = self._get_total_reviews()
        self.n_iter = round(self.total_reviews / 20) # 20 is the # of reviews per BASE_URL page (see winemag.com)
        self.url_list = []

        print(f"The total number of pages found is {self.n_iter}")

    def _get_total_reviews(self):
        bs_obj = BeautifulSoup(self.init_url.text, 'html.parser')
        return int(bs_obj.find("span", {"class": "results-count"}).text.split("of")[1].strip().replace(",", ""))
    
    def _get_reviews_urls(self):
    
        #url_list = []
        page = self.first_page
        
        while page <= self.n_pages:

            response = requests.get(url=self.init_url.format(**{"country": self.country, "n_page": page}), headers=HEADERS, timeout=randint(5,7))
            soup = BeautifulSoup(response.text, 'html.parser')
            review_urls = soup.find_all("li", {"class": "review-item"})

            for url in review_urls:
                review_url = url.find("a", {"class": "review-listing"})["href"]
                self.url_list.append(review_url)
                
            
            page += 1
            
        with open('mine_rev.txt', 'w+') as fp:
            for item in self.url_list:
                # write each item on a new line
                fp.write(f"{item}\n")

        return self.url_list
