import pyspark.sql.functions as f
from pyspark.sql import SparkSession
import re


class WordCounter:
    input_path: str
    output_path: str
    spark: SparkSession

    def __init__(self, input_path: str, output_path: str, spark: SparkSession):
        self.input_path = input_path
        self.output_path = output_path
        self.spark = spark

    def main_solution(self):
        '''
            Solution based on presentation tutorial
        '''

        res = self.spark.sparkContext.textFile(
            self.input_path,
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

        res.saveAsTextFile(self.output_path)
        print(
            *map(lambda pair: f'{pair[0]}: {pair[1]}', res.collect()[:10]),
            sep='\n',
        )

    def other_solution(self):
        '''
            My custom solution
        '''

        self.spark.read.text(INPUT_PATH).withColumn(
            'word',
            f.explode(
                f.split(
                    # Splitting strings and getting separate words
                    f.lower(f.col('value')),
                    ' ',
                )
            )
        ).filter(
            # Filtering empty strings
            f.col('value') != '',
        ).groupBy(
            'word',
        ).count().sort(
            # Counting and sorting by count descending
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


INPUT_PATH: str = 'wnp.txt'
OUTPUT_PATH: str = 'target'

# Regular Expression which checks if word only contains a-z or A-Z letters
IS_WORD = re.compile('[a-z]+', re.IGNORECASE)


if __name__ == '__main__':
    word_counter: WordCounter = WordCounter(
        INPUT_PATH,
        OUTPUT_PATH,
        SparkSession.builder.getOrCreate(),
    )

    super_log('Running main solution')
    word_counter.main_solution()

    super_log('Running other solution')
    word_counter.other_solution()
