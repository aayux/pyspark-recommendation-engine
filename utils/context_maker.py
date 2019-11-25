from pyspark import SparkConf, SparkContext, SQLContext

# create a spark context
sc = SparkContext()

# create an sql context 
# to query data files in sql like syntax
sql = SQLContext(sc)

