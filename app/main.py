#!/usr/bin/env python

from flask import Flask, \
                  request, redirect, render_template, url_for

import sys
sys.path.append('..')

from utils.get_request import *

# create a Flask instance
app = Flask(__name__)

@app.route('/')
def index():
    # get 20 random image `urls` with `numeric_asin` from the 
    # database as a list of python dictionaries
    
    """ 
    format: [{
              'numeric_asin': <numeric_asin>, 
              'title':        <title-text>, 
              'src':          <url>
            }, { ... }]
    """
    
    im_meta = get_random_books()
    return render_template('index.html', im_meta=im_meta)

@app.route('/recommendations', methods=['GET', 'POST'])
def onclick_recommend():
    if request.method == 'POST':
        numeric_asin = request.form.getlist('book')

        # call our recommendation function and return values to display
        # same format as `im_meta` in `index()`
        im_meta = get_recommendations(asin=numeric_asin)

        return render_template('recommendations.html', im_meta=im_meta)
    return

@app.route('/info')
def onclick_book():
    numeric_asin = request.form.getlist('book')
    metadata = get_book_info(asin=numeric_asin)
    return render_template('info.html', metadata=metadata)

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=9001, debug=True)
