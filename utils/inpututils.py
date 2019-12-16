#!/usr/bin/env python

def get_next_reviewer_id(uri, sql):
    last_reviewer_id = sql.read.json(f'{uri}/dumps/last_reviewer_id')
    return last_reviewer_id.collect()[0][0] + 1

def parse_json(): pass

def jsonify(df):
    return df.toJSON().collect()