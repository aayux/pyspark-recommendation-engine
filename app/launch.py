#!/usr/bin/env python

from flask import Flask, \
                  request, redirect, render_template, url_for, g

# create a Flask instance
app = Flask(__name__)

from utils.inpututils import *
from models.als import Recommender

import sys
sys.path.append('..')

@app.route('/')
def index():
    # get 20 random image `urls` with `numeric_asin` from the 
    # database as a list of python dictionaries
    
    """ 
    format: [{
              'numeric_asin': <numeric_asin>, 
              'title':        <title-text>, 
              'imUrl':          <url>
            }, { ... }]
    """

    response = model.random_sample(n=20, seed=42)
    im_meta = jsonify(response)
    print(im_meta)
    return render_template('index.html', im_meta=im_meta)

@app.route('/recommendations', methods=['GET', 'POST'])
def onclick_recommend():
    if request.method == 'POST':
        numeric_asin = request.form.getlist('book')

        # assign the next highest reviewer id
        next_reviewer_id = model.get_next_reviewer_id()
        
        # default rating for liking a book is 4.25 
        default_rating = 5 * (5 + .1) / 6
        
        # format input as list of tuples
        input_tuple = [(float(asin), 
                        next_reviewer_id, 
                        default_rating) for asin in numeric_asin]
    
        response = model.fit_transform(input_tuple)
        im_meta = jsonify(response)

        return render_template('recommendations.html', im_meta=im_meta)
    return

@app.route('/info', methods=['GET', 'POST'])
def onclick_book():
    if request.method == 'POST':
        numeric_asin = request.form.getlist('book')
        response = model.data_lookup(on='numeric_asin', key=float(numeric_asin))
        metadata = jsonify(response)
        
        return render_template('info.html', metadata=metadata)
    return

STORAGE_BUCKET = sys.argv[1]

# initialise the model object
model = Recommender(STORAGE_BUCKET)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8081, debug=True)
