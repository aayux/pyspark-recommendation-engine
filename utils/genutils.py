#!/usr/bin/env python

import pyspark

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