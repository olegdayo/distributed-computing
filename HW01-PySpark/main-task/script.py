import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

INPUT_PATH = 'wnp.txt'
OUTPUT_PATH = 'ans'

input_file = spark.sparkContext.textFile(INPUT_PATH)
map = input_file.flatMap(
    lambda line: line.split(' ')
).map(
    lambda word: (word, 1)
)

counts = map.reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile(OUTPUT_PATH)
