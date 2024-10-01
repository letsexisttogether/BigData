from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

from names import VOTES_DATASETS, unvotesSchema


def execute(spark):
    print('Executing task #1')

    stream = spark.readStream \
        .schema(unvotesSchema) \
        .option('header', 'True') \
        .csv(VOTES_DATASETS)

    query = stream.writeStream \
        .outputMode('append') \
        .format('console') \
        .start()

    query.awaitTermination()
