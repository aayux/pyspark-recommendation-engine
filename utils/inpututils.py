#!/usr/bin/env python

from .context_maker import sql

def get_next_reviewer_id(uri):
    last_reviewer_id = sql.read.json(f'{uri}/dumps/last_reviewer_id.json')
    return last_reviewer_id.collect()[0][0] + 1

def parse_json(): pass

def jsonify(df):
    return df.toJSON().collect()