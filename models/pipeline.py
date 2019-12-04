#!/usr/bin/env python

class ModelingPipline(object):
    r"""
    abstract base class for modeling pipelines
    """
    def __init__(self): raise NotImplementedError
    def fit(self): raise NotImplementedError
    def transform(self): raise NotImplementedError
    def save(self): raise NotImplementedError
    def load(self): raise NotImplementedError
