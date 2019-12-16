#!/usr/bin/env python

import os
import sys

import tensorflow as tf

from utils.inpututils import *
from utils.jsonreader import JSONDumper as d
from models.als import Recommender

from pyspark import SparkConf, SparkContext, SQLContext

def main():
    bucket_uri = sys.argv[1]
    
    # set up the Spark and Spark SQL contexts
    sc = SparkContext.getOrCreate()
    sql = SQLContext(sc)

    if not tf.io.gfile.exists(f'{bucket_uri}/dumps/train'):
        d.make_data_dict_dumps(bucket_uri)
    
    # sample input only
    next_reviewer_id = get_next_reviewer_id(bucket_uri)
    default_rating = 5 * (5 + .1) / 6
    input_tuple = [(float(10533), next_reviewer_id, default_rating), 
                   (float(2549), next_reviewer_id, default_rating), 
                   (float(4781), next_reviewer_id, default_rating)]
    
    model = Recommender(bucket_uri, sql)
    response = model.fit_transform(input_tuple)
    
    return jsonify(response)

if __name__ == '__main__':
    main()