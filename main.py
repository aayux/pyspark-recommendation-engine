#!/usr/bin/env python

import sys

from flask import Flask, g
from flask_restful import reqparse, abort, Api, Resource

from utils.inpututils import *
from models.als import Recommender

from pyspark import SparkConf, SparkContext, SQLContext

# create a Flask instance
app = Flask(__name__)

# create a Flask-RESTful API instance
api = Api(app)

# set up the Spark and Spark SQL contexts
STORAGE_BUCKET = sys.argv[1]

sc = g._sc = SparkContext.getOrCreate()
sql = SQLContext(sc)

# initialise the model object
model = Recommender(STORAGE_BUCKET, sql)

# create a parser
parser = reqparse.RequestParser()

# supply arguments to parser
parser.add_argument('query')

class app_random_books(Resource): 
    def get(self):
        response = model.random_sample(n=20, seed=42)
        return jsonify(response)

class app_recommend(Resource): 
    def get(self): 
        args = parser.parse_args()
        query = args['query']
        
        # assign the next highest reviewer id
        next_reviewer_id = get_next_reviewer_id(STORAGE_BUCKET, sql)
        
        # default rating for liking a book is 4.25 
        default_rating = 5 * (5 + .1) / 6
        
        # format input as list of tuples
        input_tuple = [(float(numeric_asin), 
                        next_reviewer_id, 
                        default_rating) for numeric_asin in query]
    
        response = model.fit_transform(input_tuple)
        return jsonify(response)

class app_book_info(Resource): 
    def get(self): 
        args = parser.parse_args()
        query = args['query']
    
        response = model.data_lookup(on='numeric_asin', key=float(query))
        return jsonify(response)

# @app.teardown_appcontext
# def teardown_sparkcontext(exception):
#     sc = getattr(g, '_sc', None)
#     if sc is not None:
#         sc.close()

# setup the API resource routing
api.add_resource(app_random_books, '/')
api.add_resource(app_recommend, '/recommendations')
api.add_resource(app_book_info, '/info')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)