from pyspark import SparkContext
import re


INPUT_PATH: str = 'wnp.txt'
OUTPUT_PATH: str = 'target'
WORDS_NUMBER: int = 10

# Regular Expression which checks if word only contains a-z or A-Z letters
IS_WORD: re.Pattern = re.compile('[a-z]+', re.IGNORECASE)


def main(spark_context: SparkContext):
    '''
        Solution based on presentation tutorial
    '''

    res = spark_context.sparkContext.textFile(
        INPUT_PATH,
    ).flatMap(
        # Splitting words
        lambda line: line.split(' '),
    ).filter(
        # Filtering words: for instance, 'abc' is a word and 'abc123.,-' is not
        lambda line: IS_WORD.match(line),
    ).map(
        lambda word: (word, 1),
    ).reduceByKey(
        lambda count1, count2: count1 + count2,
    ).sortBy(
        # Sorting by count descending
        lambda pair: pair[1],
        ascending=False,
    )

    res.saveAsTextFile(OUTPUT_PATH)
    print(
        *map(lambda pair: f'{pair[0]}: {pair[1]}',
             res.collect()[:WORDS_NUMBER]),
        sep='\n',
    )


if __name__ == '__main__':
    print('Running main solution')
    main(SparkContext(master='spark://spark-master:7077'))
