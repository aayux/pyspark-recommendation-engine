#!/usr/bin/env python

from .pipeline import ModelingPipline

from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StopWordsRemover, Tokenizer,\
                               HashingTF, IDF,\
                               Word2Vec, VectorAssembler

class TFIDFClustering(ModelingPipline):
    def __init__(self): raise NotImplementedError
    def fit(self): raise NotImplementedError
    def transform(self): raise NotImplementedError
    def save(self): raise NotImplementedError
    def load(self): raise NotImplementedError

class W2VClustering(ModelingPipline):
    def __init__(self): raise NotImplementedError
    def fit(self): raise NotImplementedError
    def transform(self): raise NotImplementedError
    def save(self): raise NotImplementedError
    def load(self): raise NotImplementedError