from pyspark.sql.functions import explode, split, concat_ws, col

from names import VOTES_DATASETS, unvotesSchema


def execute(spark):
    print('Executing task #2 version with letters')

    stream = spark.readStream \
        .schema(unvotesSchema) \
        .option('header', 'True') \
        .csv(VOTES_DATASETS)

    combined_columns = concat_ws(' ', *[col(column.name)
        for column in unvotesSchema.fields])

    split_stream = stream.select(explode(
        split(combined_columns, '')).alias('letter'))

    letter_counts_stream = split_stream.groupBy('letter') \
        .count().orderBy('letter')

    query = letter_counts_stream.writeStream \
        .outputMode('complete') \
        .format('console') \
        .start()

    query.awaitTermination()
