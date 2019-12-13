#!/usr/bin/env python

from flask import Flask
from flask_restful import reqparse, abort, Api, Resource

from .utils.inpututils import *
from .utils.preprocess import JSONDumper as d
from .models.als import Recommender

STORAGE_BUCKET = 'gs://bookreview_bucket'

# create a Flask instance
app = Flask(__name__)

# create a Flask-RESTful API instance
api = Api(app)

# create a parser
parser = reqparse.RequestParser()

# supply arguments to parser
parser.add_argument('query')

def main():
    # retrieve query text from API call
    args = parser.parse_args()
    query = args['query']

    # create a tuple from user input
    next_reviewer_id = get_next_reviewer_id(bucket_uri)
    default_rating = 5 * (5 + .1) / 6
    input_tuple = [(int(asin), 
                    next_reviewer_id, 
                    default_rating) for asin in query]
    
    # instantiate the model object
    model = Recommender(STORAGE_BUCKET)
    
    # get recommendations
    response = model.fit_transform(input_tuple)

    # return json response
    return jsonify(response)

# setup the API resource routing
api.add_resource(main, '/')

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)
