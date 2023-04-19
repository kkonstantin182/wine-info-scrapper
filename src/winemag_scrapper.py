import json
import requests
from bs4 import BeautifulSoup
import pandas as pd
from random import randint
from pathlib import Path 
from configuration import FIRST_PAGE, BASE_URL, DATA_DIR, FILE_NAME_URLS, FILE_NAME_REVIEWS, HEADERS, DATA_DIR, FILE_NAME_URLS, FILE_NAME_REVIEWS
from configuration import get_project_root


class Scrapper:

    def __init__(self, country):

        self.country = country
        self.n_pages = None
        self.first_page = FIRST_PAGE
        self.init_url = BASE_URL.format(**{"country": self.country, "n_page": self.first_page})
        self.total_reviews = self._get_total_reviews()
        self.n_iter = round(self.total_reviews / 20) # 20 is the # of reviews per BASE_URL page (see winemag.com)
        self.url_list = []

        print(f"The total number of pages found is {self.n_iter}")

    def _get_total_reviews(self):
        response = requests.get(url=self.init_url, headers=HEADERS)
        bs_obj = BeautifulSoup(response.text, 'html.parser')
        return int(bs_obj.find("span", {"class": "results-count"}).text.split("of")[1].strip().replace(",", ""))
    
    def set_n_pages(self, n_pages):
        self.n_pages = n_pages
        
    def _get_reviews_urls(self):
    
        page = self.first_page
        
        while page <= self.n_pages:

            response = requests.get(url=self.init_url.format(**{"country": self.country, "n_page": page}), headers=HEADERS, timeout=randint(5,7))
            soup = BeautifulSoup(response.text, 'html.parser')
            review_urls = soup.find_all("li", {"class": "review-item"})

            for url in review_urls:
                review_url = url.find("a", {"class": "review-listing"})["href"]
                self.url_list.append(review_url)
                
            
            page += 1

        output_folder =  get_project_root() / DATA_DIR 
        output_folder.mkdir(exist_ok=True)

        with open(output_folder / FILE_NAME_URLS,  "w+") as f:
            for url in self.url_list:
                f.write(f"{url}\n")

        return self.url_list
    
    def extract_reviews(self):
            
        results = pd.DataFrame(columns=[
            'wine_name', 
            'winery', 
            'type', 
            'price', 
            'designation', 
            'variety1', 
            'variety2', 
            'reviewer', 
            'review', 
            'rating'])

        for url in self.url_list:

            request = requests.get(url=url, headers=HEADERS, timeout=(randint(3, 5), randint(5, 7))) # For winemag.com read timout must be >= 5 
            soup = BeautifulSoup(request.text, 'html.parser')


            # Some data are stored in json
            soup_json = soup.find_all("script", type="application/ld+json")[1]
            json_data = json.loads(soup_json.string)

            # __Extract from json__

            # Wine name
            wine_name = json_data["name"]

            # Wine type
            wine_type = json_data["category"]

            # Reviewer's name
            reviewer_name = json_data["review"]["author"]["name"]

            # Review text
            review_text = json_data["review"]["reviewBody"]
            
            # Rating
            rating = json_data["review"]["reviewRating"]["ratingValue"]

            # __Extract from html__

            # Find the place in the document ralated to the info needed
            html_data = soup.find("ul", {"class": "primary-info"}).find_all("li", {"class": "row"})

            # Extract price value
            price = html_data[1].find_all("span")[1].find("span").text.split(",")[0]
            

            # Extract Designation
            designation = html_data[2].find_all("span")[1].text
            
            # Extract variery (grape type)
            grape_list = html_data[3].find_all("span")[1].text.split(",")
            
            grape1 = html_data[3].find_all("span")[1].text.split(",")[0]
            if len(grape_list) != 1:
                grape2 = html_data[3].find_all("span")[1].text.split(",")[1]
            else: grape2 = None

            # Extract winery name 
            winery = html_data[5].find_all("span")[1].text
            
            data = pd.DataFrame.from_dict(
                {
                "wine_name": [wine_name],
                "winery": [winery],
                "type": [wine_type],
                "price": [price],
                "designation": [designation],
                "variety1": [grape1],
                "variety2": [grape2], 
                "reviewer": [reviewer_name],
                "review" : [review_text],
                "rating": [rating]

            }
            )

            results = pd.concat([results, data], axis=0)

        output_folder =  get_project_root() / DATA_DIR 
        output_folder.mkdir(exist_ok=True)
        results.to_csv(f"{FILE_NAME_REVIEWS}.csv")


   
         

