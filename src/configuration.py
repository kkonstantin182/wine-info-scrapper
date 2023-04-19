from pathlib import Path

def get_project_root() -> Path:
    return Path(__file__).parent.parent


##### Related to teh scrapper ######
HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36"
    )
}

FIRST_PAGE = 1
BASE_URL = "https://www.winemag.com/?s=&drink_type=wine&country={country}&page={n_page}&sort_by=pub_date_web&sort_dir=desc&search_type=reviews"
DATA_DIR = "data"
FILE_NAME_URLS = "winemag-urls.txt"
FILE_NAME_REVIEWS = "winemag-data.csv"