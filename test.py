#!/usr/bin/env python

import os
import sys

from utils.inpututils import *
from utils.preprocess import JSONDumper as d

# from models.als import Recommender

def main():
    bucket_uri = sys.argv[1]
    
    # make the data dump
    if not os.path.exists(f'{bucket_uri}/dumps/processed.json'):
        d.make_data_dict_dumps(d, bucket_uri)
        
        import subprocess
        subprocess.call(['bash scripts/chunker.sh'])

    # sample input, real input will require parsing json
    # next_reviewer_id = get_next_reviewer_id(bucket_uri)
    # default_rating = 5 * (5 + .1) / 6
    # input_tuple = [(10533, next_reviewer_id, default_rating), 
    #                (2549, next_reviewer_id, default_rating), 
    #                (4781, next_reviewer_id, default_rating)]
    
    # model = Recommender(bucket_uri)
    # response = model.fit_transform(input_tuple)

    # # jsonify DataFrame and send to website
    # return jsonify(response)

if __name__ == '__main__':
    main()