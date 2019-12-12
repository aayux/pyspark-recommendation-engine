#!/usr/bin/env python

import time
import pyspark
import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.storagelevel import StorageLevel

import numpy as np

from .context_maker import sql
from .genutils import *

class JSONReader(object):
    r"""
    """
    def __init__(self):
        self.data_dict = None
        self.data = None
    
    def read_json(self, uri, dict_format=True):
        r"""
        `dict_format` (bool): read metabooks and reviews data_dict into 
                              a dictionary containing 
                              `pyspark.sql.dataframe.DataFrame`
        """
        if dict_format:
            mfile = f'{uri}/metaBooks.json'
            rfile = f'{uri}/reviews_Books_5.json'
            
            self.data_dict = dict([('m', sql.read.json(mfile)),
                                   ('r', sql.read.json(rfile))])
        else:
            dfile = f'{uri}/processed.json'
            return sql.read.json(dfile)

    def write_json(self, uri):
        try:
            self.data.write.json.save(f'{uri}/processed.json')
        except:
            # log event
            return False 
        return True

class JSONPreprocesser(JSONReader):
    r"""
    """
    def __init__(self, uri):
        super().__init__()
        self.read_json(uri)
    
    def transform(self): 
        # drop unnecessary or redundant columns
        self.data_dict['r'] = self.data_dict['r'].drop('_corrupt_record', 
                                                       'unixReviewTime')
        self.data_dict['m'] = self.data_dict['m'].drop('_corrupt_record', 
                                                       'brand', 'categories')

        self.data_dict['r'] = self.data_dict['r'].repartition(63)
        self.data_dict['m'] = self.data_dict['m'].repartition(31)

        self.data_dict['r'] = self.data_dict['r'].persist(StorageLevel.MEMORY_AND_DISK)
        self.data_dict['m'] = self.data_dict['m'].persist(StorageLevel.MEMORY_AND_DISK)


        # split the column `helpful`
        self.data_dict['r'] = self.data_dict['r'].select(self.data_dict['r'].columns + \
                                [F.expr(f'helpful[{i}]') for i in range(0, 2)])
        
        # calculate the total meta-reviews
        self.data_dict['r'] = self.data_dict['r']\
                                  .join(F.broadcast(self.data_dict['r']\
                                  .groupBy('asin').sum('helpful[1]')), on='asin', 
                                                                       how='left')
        
        # the `help_factor` for a review is the number of people who found the review helpful
        # to the total number of meta-reviewers
        self.data_dict['r'] = self.data_dict['r'].withColumn('help_factor', 
                                F.col('helpful[0]') / F.col('sum(helpful[1])'))

        self.data_dict['r'] = self.data_dict['r'].fillna(0., subset=['help_factor'])
        self.data_dict['r'] = self.data_dict['r'].drop('helpful[0]', 
                                             'helpful[1]', 'sum(helpful[1])')

        # the actual rating is a function of the help-factor and the overall rating
        self.data_dict['r'] = self.data_dict['r'].withColumn('rating', 
                                                5 * (F.col('overall') + \
                                                    F.col('help_factor')) / 6)

        # average rating by book
        self.data_dict['r'] = self.data_dict['r']\
                              .join(F.broadcast(rename_columns(
                                                    self.data_dict['r']\
                                                        .groupBy('asin')\
                                                        .avg('overall'), 
                                                    ['asin', 'average_rating'])), 
                            on='asin', how='left')

        # keep the most helful reviews
        window = Window.partitionBy('asin')
        most_helpful = self.data_dict['r'].withColumn('top_review', 
                        F.max('help_factor').over(window))\
                         .where(F.col('help_factor') == F.col('top_review'))\
                         .select('asin', 'reviewText', 'summary', 'help_factor')\
                         .dropDuplicates(['asin', 'help_factor'])\
                         .drop('help_factor').persist(StorageLevel.DISK_ONLY)
        most_helpful = most_helpful.withColumnRenamed('reviewText', 
                                                      'topReviewText')
        most_helpful = most_helpful.withColumnRenamed('summary', 
                                                      'topReviewSummary')

        # dataframes are too large for broadcast join
        self.data_dict['r'] = self.data_dict['r'].join(most_helpful, on='asin', 
                                                                     how='left')

        most_helpful.unpersist(blocking=False)

        self.data_dict['r'] = self.data_dict['r'].drop('helpful', 'overall', 
                                                       'reviewTime', 
                                                       'reviewerName', 
                                                       'reviewText', 'summary', 
                                                       'help_factor')

        # replace absent titles with Untitled
        self.data_dict['m'] = self.data_dict['m'].fillna('Untitled', 
                                                         subset=['title'])
        
        # replace with empty string, useful when preprocessing text
        # self.data_dict['m'] = self.data_dict['m'].fillna('', 
        #                                                  subset=['description'])

        # replace with "#" to represent broken links
        self.data_dict['m'] = self.data_dict['m'].fillna('#', subset=['imUrl'])

        
        # extract book's `salesRank` from the dictionary structure
        # self.data_dict['m'] = self.data_dict['m'].withColumn('salesRank', 
        #                                            F.expr("salesRank['Books']"))

        # average fill empty `salesRank` values: the modeling assumption being 
        # that any unranked book is equally likely to be on either 
        # end of the ranking
        # avg_rank = self.data_dict['m'].agg({'salesRank': 'avg'}).collect()[0][0]

        # # add some random noise to the ratings
        # self.data_dict['m'] = self.data_dict['m'].withColumn('salesRank', 
        #                         F.coalesce(F.col('salesRank'), 
        #                         F.when(F.rand() > 0.5, 
        #                         F.round(avg_rank + F.rand() * 10))\
        #                         .otherwise(F.round(avg_rank - F.rand() * 10))))

        # split the `related` column
        # TO DO: automatically fetch these
        # related_keys = ['also_bought', 'also_viewed', 
        #                 'bought_together', 'buy_after_viewing']
        # column_names = self.data_dict['m'].columns + related_keys
        # self.data_dict['m'] = rename_columns(self.data_dict['m'].\
        #                     select(self.data_dict['m'].columns + \
        #                     [F.expr(f'related["{key}"]') for key in related_keys]), 
        #                     column_names)
        
        self.data_dict['m'] = self.data_dict['m'].drop('related', 'salesRank')

        self.data = self.data_dict['m'].join(self.data_dict['r'], 
                                             on='asin', how='left')

        self.data = self.data.repartition(127)

        self.data_dict['r'].unpersist(blocking=False)
        self.data_dict['m'].unpersist(blocking=False)

        self.data = self.data.persist(StorageLevel.DISK_ONLY)

        # print(f'Created table of size {self.data.shape()}')
        start = time.time()
        self.data = map_to_numeric_large(self.data, 'asin')
        # self.data = string_indexer(self.data, 'asin')
        print(f'Elapsed: {time.time() - start}')
        start = time.time()
        self.data = map_to_numeric_large(self.data, 'reviewerID')
        # self.data = string_indexer(self.data, 'reviewerID')
        print(f'Elapsed: {time.time() - start}')
        
        last_reviewer_id = self.data.agg({'numeric_reviewerID': 'max'})
        return last_reviewer_id

class JSONDumper(JSONReader):
    r"""
    """

    @classmethod
    def make_data_dict_dumps(cls, bucket_uri):
        r"""
        """
        preprocesser = JSONPreprocesser(bucket_uri)
        last_reviewer_id = preprocesser.transform()
        last_reviewer_id.write.mode('overwrite')\
                        .json(f'{bucket_uri}/last_reviewer_id.json')
        return cls.write_json(cls, bucket_uri)