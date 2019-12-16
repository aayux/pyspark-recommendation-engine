#!/usr/bin/env python

import requests

DATAPROC = '<dataproc-ext-ip>'

def get_random_books():

    url = f'http://{DATAPROC}:8080'
    response = requests.get(url)

    return response.json()


def get_recommendations(asin):

    url = f'http://{DATAPROC}:8080/recommendations'
    response = requests.get(url, params=asin)

    return response.json()


def get_book_info(asin):
    url = f'http://{DATAPROC}:8080/info'
    response = requests.get(url, params=asin)

    return response.json()


if __name__ == '__main__': print(get_random_books())