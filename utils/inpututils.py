#!/usr/bin/env python

from .context_maker import sql

import numpy as np

def get_next_reviewer_id(uri):
    return np.load(f'{uri}/dumps/next_reviewer_id.npy')

def parse_json(): pass

def jsonify(df):
    return df.toJSON().collect()