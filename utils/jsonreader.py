#!/usr/bin/env python

from .context_maker import sql
from .genutils import *

import pyspark
import pyspark.sql.functions as F
from pyspark.sql import Window

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
        mfile = '{uri}/chunks/metabooks/metabooks0*.json'
        rfile = '{uri}/chunks/reviews/reviews0*.json'
        
        self.data_dict = dict(('m', sql.read.json(mfile)),
                              ('r', sql.read.json(rfile)))

    def write_json(self, uri):
        try:
            self.data.write.json.save(f'{uri}/dumps/data.json')
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
        
        # drop rows without any titles
        if self.data_dict['m'].where(F.isnull(F.col('title'))).count() > 0:
            # log the number of rows being dropped
            self.data_dict['m'] = self.data_dict['m'].dropna(subset=['title'])
        
        # replace with empty string, useful when preprocessing text
        self.data_dict['m'] = self.data_dict['m'].fillna("", 
                                                         subset=['description'])

        # replace with "#" to represent broken links
        self.data_dict['m'] = self.data_dict['m'].fillna("#", subset=['imUrl'])

        
        # extract book's `salesRank` from the dictionary structure
        self.data_dict['m'] = self.data_dict['m'].withColumn('salesRank', 
                                                   F.expr("salesRank['Books']"))

        
        # average fill empty `salesRank` values: the modeling assumption being 
        # that any unranked book is equally likely to be on either 
        # end of the ranking
        avg_rank = self.data_dict['m'].agg({'salesRank': 'avg'}).collect()[0][0]

        # add some random noise to the ratings
        self.data_dict['m'] = self.data_dict['m'].withColumn('salesRank', 
                                F.coalesce(F.col('salesRank'), 
                                F.when(F.rand() > 0.5, 
                                F.round(avg_rank + F.rand() * 10))\
                                .otherwise(F.round(avg_rank - F.rand() * 10))))
        
        # reduce to log (base 10)
        self.data_dict['m'] = self.data_dict['m'].withColumn('logSalesRank', 
                                                   F.log10(F.col('salesRank')))

        # split the `related` column
        # TO DO: automatically fetch these
        related_keys = ['also_bought', 'also_viewed', 
                        'bought_together', 'buy_after_viewing']
        column_names = self.data_dict['m'].columns + related_keys
        self.data_dict['m'] = rename_columns(self.data_dict['m'].\
                            select(self.data_dict['m'].columns + \
                            [F.expr(f'related["{key}"]') for key in related_keys]), 
                            column_names)
        self.data_dict['m'] = self.data_dict['m'].drop('related')

        # join the text and description fields
        self.data_dict['m'] = self.data_dict['m'].withColumn('text', 
                                        F.concat(F.lit('<BOS> '), F.col('title'), 
                                        F.lit(' <PAD> '), F.col('description'), 
                                        F.lit(' <EOS>'))).drop('title', 
                                                               'description')

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

        # average rating by book
        self.data_dict['r'] = self.data_dict['r']\
                              .join(F.broadcast(rename_columns(
                                                    self.data_dict['r']\
                                                        .groupBy('asin')\
                                                        .avg('overall'), 
                                                    ['asin', 'average_rating'])), 
                            on='asin', how='left')
        self.data_dict['r'] = self.data_dict['r'].fillna(0., subset=['help_factor'])
        self.data_dict['r'] = self.data_dict['r'].drop('overall', 'helpful[0]', 
                                             'helpful[1]', 'sum(helpful[1])')

        # keep the most helful reviews
        window = Window.partitionBy('asin')
        self.data_dict['r'] = self.data_dict['r'].withColumn('top_review', 
                            F.max('help_factor').over(window))\
                            .where(F.col('help_factor') == F.col('top_review'))\
                    .drop('top_review').dropDuplicates(['asin', 'help_factor'])
        
        self.data = self.data_dict['m'].join(F.broadcast(self.data_dict['r']), 
                                             on='asin', how='left')

class JSONDumper(JSONReader):
    r"""
    """

    @classmethod
    def make_data_dict_dumps(cls, bucket_uri):
        r"""
        """
        preprocesser = JSONPreprocesser(bucket_uri)
        preprocesser.transform()
        return cls.write_json(cls, bucket_uri)