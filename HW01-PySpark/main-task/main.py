import pyspark.sql.functions as f
from pyspark.sql import SparkSession

INPUT_PATH: str = 'wnp.txt'
OUTPUT_PATH: str = 'target'


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

        self.spark.sparkContext.textFile(self.input_path).flatMap(
            lambda line: line.split(' ')
        ).map(
            lambda word: (word, 1)
        ).reduceByKey(
            lambda count1, count2: count1 + count2
        ).saveAsTextFile(self.output_path)

    def other_solution(self):
        '''
            My custom solution
        '''

        self.spark.read.text(INPUT_PATH).withColumn(
            'word',
            f.explode(
                f.split(  # Splitting strings and getting separate words
                    f.lower(f.col('value')),
                    ' ',
                )
            )
        ).filter(  # Filtering empty strings
            f.col('value') != ''
        ).groupBy(
            'word',
        ).count().sort(  # Counting and sorting by count descending
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
    word_counter: WordCounter = WordCounter(
        INPUT_PATH,
        OUTPUT_PATH,
        SparkSession.builder.getOrCreate(),
    )

    super_log('Running main solution')
    word_counter.main_solution()

    super_log('Running other solution')
    word_counter.other_solution()
