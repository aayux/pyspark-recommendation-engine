#!/usr/bin/env python

import pyspark
import pyspark.sql.functions as F

from pyspark.sql import Window
from pyspark.ml.feature import StringIndexer

def spark_shape(self):
    r"""
    helper function, returns the shape of a `pyspark.sql.dataframe.DataFrame`
    in Pandas format.
    """
    return (self.count(), len(self.columns))

# function works as a callable for DataFrame object
pyspark.sql.dataframe.DataFrame.shape = spark_shape

def rename_columns(df, replace_with):
    r"""
    renames a DataFrame with column names replaced 
    with values specified in `replace_with`.
    """
    try:
        assert len(df.columns) == len(replace_with)
        df = df.toDF(*replace_with)
    except AssertionError:
        print(f'Warning: length of `replace_with` does not match the number of '
              f'columns, returning DataFrame, without renaming columns.')
    return df

def map_to_numeric_small(df, column):
    r""" 
    map string IDs to numeric  values for small datasets
    NOTE: this will not work with larger datasets because there is no
    column to parition by
    
    Reference: https://stackoverflow.com/a/55940139

    NOTE: Can we parition by current column
    """
    mapID = df.select(column).distinct()
    window = Window.partitionBy(column).orderBy(F.monotonically_increasing_id())
    mapID = mapID.withColumn(f'numeric_{column}',
                             F.row_number().over(window) - 1)
    df = df.join(F.broadcast(mapID), on=column, how='left')
    return df

def map_to_numeric_large(df, column):
    r""" map string IDs to numeric  values for large datasets
    """
    mapID = df.select(column).distinct()

    mapID = mapID.rdd.zipWithIndex().map(lambda r: (r[0][0], r[1] + 1)).toDF()

    mapID = mapID.withColumnRenamed('_1', column)
    mapID = mapID.withColumnRenamed('_2', f'numeric_{column}')
    
    df = df.join(mapID, on=column, how='left')
    return df

def string_indexer(df, column):
    r""" baseline comparison for our map function
    """
    indexer = StringIndexer(inputCol=column, outputCol=f'numeric_{column}')
    return indexer.fit(df).transform(df)