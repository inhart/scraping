from pymongo import MongoClient
import requests
from bs4 import BeautifulSoup
from datetime import datetime

# Variables globales
movie_counter = 0
api_counter = 0
html_parser = 'html.parser'


def make_request(url, params=None):
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response
    else:
        log(f'Request error: {response.status_code}')
        return None


def log(message):
    with open('log.txt', 'a') as f:
        try:
            f.write(f'{datetime.now()} {message}\n')
        except Exception as e:
            print(f'Error saving log: {e}')


def insert_to_mongo(db, document):
    try:
        db.insert_one(document)
    except Exception as e:
        log(f'Error inserting document: {e}')


def increment_counter(counter_name):
    global movie_counter, api_counter
    if counter_name == 'movie':
        movie_counter += 1
    elif counter_name == 'api':
        api_counter += 1


def scrape_movie(enlace, name, category, db):
    response = make_request(enlace)
    if response is None:
        return

    soup = BeautifulSoup(response.content, html_parser)
    comments_section = soup.find_all('div', 'separator')
    reactions = [num.get_text() for num in soup.find_all('span', "count-num")]

    if not comments_section:
        comment = soup.find_all('p')[1].get_text()
    else:
        comment = comments_section[-1].get_text().split('\n')[0]

    movie = {
        '_id': movie_counter,
        'title': name[:-7],
        'year': name[-5:-1],
        'category': category,
        'like': reactions[0],
        'dislike': reactions[1],
        'love': reactions[2],
        'shit': reactions[3],
        'link': enlace,
        'comment': comment
    }

    insert_to_mongo(db, movie)
    increment_counter('movie')


def scrape_category(db, enlace, category):
    response = make_request(enlace)
    if response is None:
        return

    soup = BeautifulSoup(response.content, html_parser)
    posts = soup.find_all('div', 'latestPost-inner')

    for post in posts:
        name = post.find('a')['title']
        link = post.find('a')['href']
        scrape_movie(link, name, category, db)


def ingest_movies(db):
    base_url = "https://www.blogdepelis.top/"
    response = make_request(base_url)
    if response is None:
        return

    soup = BeautifulSoup(response.content, html_parser)
    categories = [a for a in soup.find_all('a') if "category" in a['href']]

    for category in categories:
        category_link = category['href']
        category_name = category.get_text()
        response = make_request(category_link)
        if response is None:
            continue

        soup = BeautifulSoup(response.content, html_parser)
        total_pages = int(soup.find_all('a', 'page-numbers')[-2].get_text())

        for i in range(1, total_pages + 1):
            page_link = f'{category_link}/page/{i}'
            scrape_category(db, page_link, category_name)


def ingest_api_data(db):
    base_url = "https://api.euskadi.eus/culture/events/v1.0/events/"
    params = {
        '_elements': 1,
        '_page': 1,
        'type': 9
    }

    response = make_request(base_url, params)
    if response is None:
        return

    data = response.json()
    total_pages = data['totalPages']

    for i in range(1, total_pages + 1):
        params['_page'] = i
        response = make_request(base_url, params)
        if response is None:
            continue

        event = response.json()['items'][0]
        event['_id'] = api_counter
        insert_to_mongo(db, event)
        increment_counter('api')


def main():
    client = MongoClient("localhost", 27017)
    db = client.IDD
    blog_collection = db['blog']
    api_collection = db['api']

    ingest_api_data(api_collection)
    ingest_movies(blog_collection)


if __name__ == '__main__':
    main()
