import pyspark.sql.functions as f
from pyspark.sql import SparkSession


INPUT_PATH: str = 'wnp.txt'
OUTPUT_PATH: str = 'target'


def main():
    '''
        Solution based on presentation tutorial
    '''

    spark: SparkSession = SparkSession.builder.getOrCreate()

    input_file = spark.sparkContext.textFile(INPUT_PATH)
    mp = input_file.flatMap(
        lambda line: line.split(' ')
    ).map(
        lambda word: (word, 1)
    )

    counts = mp.reduceByKey(lambda a, b: a + b)
    counts.saveAsTextFile(OUTPUT_PATH)


def other():
    '''
        My custom solution
    '''
        
    spark: SparkSession = SparkSession \
        .builder \
        .getOrCreate()

    spark \
        .read \
        .text(INPUT_PATH) \
        .withColumn(
            'word',
            f.explode(
                f.split(
                    # Splitting strings and getting separate words
                    f.lower(f.col('value')),
                    ' ',
                )
            )
        ).filter(  # Filtering empty strings
            f.col('value') != ''
            # Counting and sorting by count descending
        ).groupBy('word').count().sort(
            'count',
            ascending=False,
        ).show()


def super_log(message: str):
    print('-' * 100)
    print('-' * 100)
    print('-' * 100)
    print(message)
    print('-' * 100)
    print('-' * 100)
    print('-' * 100)


if __name__ == '__main__':
    super_log('Running main solution')
    main()

    super_log('Running other solution')
    other()
