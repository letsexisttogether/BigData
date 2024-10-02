from pyspark.sql.functions import explode, split, col

from names import TEXT_DATASETS


def execute(spark):
    print('Executing task #2 version with words for text files')

    stream = spark.readStream \
        .format('text') \
        .load(TEXT_DATASETS)

    split_stream = stream.select(explode(
        split(col('value'), ' ')).alias('word'))

    word_counts_stream = split_stream.groupBy('word').count()

    query = word_counts_stream.writeStream \
        .outputMode('complete') \
        .format('console') \
        .start()

    query.awaitTermination()
