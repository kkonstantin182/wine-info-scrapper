import json
import requests
from bs4 import BeautifulSoup
import pandas as pd
from random import randint
from pathlib import Path 
from configuration import FIRST_PAGE, BASE_URL, DATA_DIR, FILE_NAME_URLS, FILE_NAME_REVIEWS, HEADERS, DATA_DIR, FILE_NAME_URLS, FILE_NAME_REVIEWS, MAX_RETRIES
from configuration import get_project_root
from multiprocessing import Pool
from tqdm import tqdm
import time

def timer(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print("Operation is completed")
        print(f"It took: {end_time - start_time:.5f} seconds")
        return result
    return wrapper


class Scraper:

    def __init__(self, country, header=HEADERS, first_page=FIRST_PAGE):

        self.country = country
        self._header = header
        self.first_page = first_page
        self._init_url = BASE_URL
        self.url_list = []
        self.total_reviews = self._get_total_reviews()
        self._n_iter = round(self.total_reviews / 20) # 20 is the # of reviews per BASE_URL page (see winemag.com)
        self._n_pages = 1
        self.max_retries = MAX_RETRIES
        
        print(f"The total number of pages found is {self._n_iter}")

    
    @property
    def n_pages(self):
        return self._n_pages
    
    @n_pages.setter
    def n_pages(self, n_pages):
        if (n_pages >=1) & (n_pages <= self._n_iter):
            self._n_pages = n_pages
        else:
            raise ValueError(f"The number of pages must be within the range [1, {self._n_iter}]")
    

    def _get_total_reviews(self):
        response = requests.get(url=self._init_url.format(**{"country": self.country, "n_page": self.first_page}), headers=self._header)
        bs_obj = BeautifulSoup(response.text, 'html.parser')
        return int(bs_obj.find("span", {"class": "results-count"}).text.split("of")[1].strip().replace(",", ""))
    
    def _get_review_urls_on_page(self, page):
        
        num_retries = 0
        urls = []

        while True: 

            try:
                response = requests.get(url=self._init_url.format(**{"country": self.country, "n_page": page}), headers=self._header, timeout=randint(5,7))
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')
                review_urls = soup.find_all("li", {"class": "review-item"})

                for url in review_urls:
                    review_url = url.find("a", {"class": "review-listing"})["href"]
                    urls.append(review_url)
                
                return urls

            except (requests.exceptions.HTTPError, requests.exceptions.RequestException) as e:
                print(f"Request failed. Retrying ({num_retries+1}/{self.max_retries})...")
                num_retries += 1

                if num_retries >= self.max_retries:
                    raise e
                
                time.sleep(randint(5, 15))
                continue
        

    @timer
    def _get_reviews_urls(self):
        
        print("Extracting urls")
        pages = range(self.first_page, self.n_pages + 1)
        
        with Pool() as pool:
            results = pool.map(self._get_review_urls_on_page, pages)
    

        for url_batch in results:
            self.url_list += url_batch

        output_folder =  get_project_root() / DATA_DIR 
        output_folder.mkdir(exist_ok=True)

        with open(output_folder / FILE_NAME_URLS,  "w+") as f:
            for url in self.url_list:
                f.write(f"{url}\n")


    def _extract_review(self, url):    
        num_retries = 0
        while True:
            try:
                request = requests.get(url=url, headers=self._header, timeout=(randint(3, 5), randint(5, 7))) # For winemag.com read timout must be >= 5 
                soup = BeautifulSoup(request.text, 'html.parser')

                # Some data are stored in json
                soup_json = soup.find_all("script", type="application/ld+json")[1]
                json_data = json.loads(soup_json.string)

                ###### Extract from json ######

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

                dict1 = {
                    "Wine_name": [wine_name],
                    "Type": [wine_type],
                    "Reviewer": [reviewer_name],
                    "Review" : [review_text],
                    "Rating": [rating],

                }
                ###### Extract from html #####

                # Find the place in the document ralated to the info needed
                html_data = soup.find("ul", {"class": "primary-info"}).find_all("li", {"class": "row"})
                
                dict2 = {}
                for i in range(len(html_data)):

                    if html_data[i].find_all("span")[0].text == "Price":
                        dict2[html_data[i].find_all("span")[0].text] = html_data[1].find_all("span")[1].find("span").text.split(",")[0]
                    else:
                        dict2[html_data[i].find_all("span")[0].text] = html_data[i].find_all("span")[1].text

                data = pd.DataFrame.from_dict({**dict1,**dict2})

                return data
            
            except (requests.exceptions.HTTPError, requests.exceptions.RequestException) as e:
                num_retries += 1
                print(f"Connection failed, retrying ({num_retries}/{self.max_retries})")
                if num_retries >= self.max_retries:
                    raise e
                time.sleep(randint(5, 15))
                continue
            
    @timer
    def extract_reviews(self):
        print("Extracting reviews")
        with Pool() as pool:
            results = list(tqdm(pool.imap(self._extract_review, self.url_list), total=len(self.url_list)))

        output_folder = get_project_root() / DATA_DIR 
        output_folder.mkdir(exist_ok=True)
        results = pd.concat(results, axis=0)
        results.to_csv(output_folder / FILE_NAME_REVIEWS, index=False)
       
        

if __name__ == "__main__":

    country = input("Specify a country: ")
    scr_obj = Scraper(country)
    n_pages = int(input("Specify the number of pages to scrape: "))
    scr_obj.n_pages = n_pages
    scr_obj._get_reviews_urls()
    scr_obj.extract_reviews()
