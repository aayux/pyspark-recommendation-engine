#!/usr/bin/env python

import pyspark
import pyspark.sql.functions as F
from pyspark.sql import Window

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

def map_to_numeric(df, column):
    r""" map string IDs to numeric  values
    """
    mapID = df.select(column).distinct()
    mapID = mapID.withColumn(f'numeric_{column}',   
                             F.row_number().over(
                                 Window.orderBy(
                                     F.monotonically_increasing_id())) - 1)
    df = df.join(F.broadcast(mapID), on=column, how='left')
    return df