#!/usr/bin/env python

from context_maker import sql

class JSONReader(object):
    r"""
    """
    def __init__(self): pass
    
    @staticmethod
    def make_data_dumps(input_uri, output_uri):
        r"""
        """
        # coalsce/join meta-books and reviews data
        if isinstance(input_uri, list):
            data = [sql.read.json(uri) for uri in input_uri]
        else:
            data = sql.read.json(input_uri)
        
        # transform datatset
        # preprocesser = Preprocesser(data)
        # data = preprocesser.transform(data)

        data.write.json.save(f'{output_uri}/dumps/data.json')

class Preprocesser(object):
    r"""
    """
    def __init__(self, data):
        self.data = data
    
    def transform(self): pass