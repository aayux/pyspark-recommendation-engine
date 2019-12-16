from pyspark.sql import SparkSession, SQLContext

# create a spark context
spark = SparkSession.builder.appName('recommendation_app').getOrCreate()

sc = spark.sparkContext

# create an sql context 
# to query data files in sql like syntax
sql = SQLContext(sc)