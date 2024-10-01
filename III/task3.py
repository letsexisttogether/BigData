from pyspark.sql.functions import explode, split, col

from names import GAMES_DATASETS, gamesSchema 


def execute(spark):
    print('Executing task #2 version with words')

    stream = spark.readStream \
        .schema(gamesSchema) \
        .option('header', 'True') \
        .csv(GAMES_DATASETS)

    split_stream = stream.select(explode(
        split(col('gamename'), ' ')).alias('word'))

    word_counts_stream = split_stream.groupBy('word').count()

    query = word_counts_stream.writeStream \
        .outputMode('complete') \
        .format('console') \
        .option('truncate', False) \
        .start()

    query.awaitTermination()
