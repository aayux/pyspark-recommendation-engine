#!/usr/bin/env python

import pyspark.sql.functions as F

from pyspark.ml.recommendation import ALS
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

from ..utils.jsonreader import JSONReader
from ..utils.context_maker import sql

class Recommender(object):
    def __init__(self, uri, method='ALS'):
        reader = JSONReader()
        self.data = reader.read_json(uri, dict_format=False)
        self.train = self.data.select('numeric_asin', 
                                      'numeric_reviewerID', 
                                      'rating')
        # self.method = method
    
    def fit_transform(self, input_tuple):
        schema = StructType([StructField('numeric_asin', IntegerType(), False),
                             StructField('numeric_reviewerID', IntegerType(), False), 
                             StructField('rating', FloatType(), False)])
        user_input = sql.createDataFrame(input_tuple, schema)
        self.train = self.data.union(user_input)

        # build the recommendation model using ALS on the training data
        # note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
        als = ALS(maxIter=5, regParam=0.01, 
                  userCol='numeric_reviewerID', 
                  itemCol='numeric_asin', ratingCol='rating', 
                  coldStartStrategy='drop')
        
        model = als.fit(self.train)
        recommendations = model.recommendForUserSubset(
                                    user_input.select(
                                        'numeric_reviewerID').distinct(), 
                                    5).toPandas() # is `toPandas()` required?
        recommendations = [row['numeric_asin'] for row in \
                            recommendations.recommendations.values[0]]
        
        response = self.data.dropDuplicates(['asin'])\
                            .filter(F.col('numeric_asin')\
                            .isin(recommendations))\
                            .drop('numeric_asin', 
                                  'numeric_reviewerID', 
                                  'rating')
        
        return response
