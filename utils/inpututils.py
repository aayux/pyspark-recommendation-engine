#!/usr/bin/env python

def parse_json(): pass

def jsonify(df):
    return df.toJSON().collect()