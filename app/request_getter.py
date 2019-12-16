#!/usr/bin/env python

import requests

DATAPROC = '104.199.122.115'

proxies = {
    'http': 'socks5://localhost:1080',
    'https': 'socks5://localhost:1080'
}

def get_random_books():

    url = f'http://{DATAPROC}:8080'
    response = requests.get(url, proxies=proxies)

    return response.json()


def get_recommendations(asin):

    url = f'http://{DATAPROC}:8080/recommendations'
    response = requests.get(url, params=asin, proxies=proxies)

    return response.json()


def get_book_info(asin):
    url = f'http://{DATAPROC}:8080/info'
    response = requests.get(url, params=asin, proxies=proxies)

    return response.json()