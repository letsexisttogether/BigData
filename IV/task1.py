from pyspark.sql import SparkSession

from stream_generator import StreamDataGenerator
from names import JSON_DATASETS, schema


def execute(spark):
    print('Executing Task #1')

    data = [
        (1, 'Sasha', 'KPI', 70, '2024-10-15 11:13:10'),
        (2, 'Michael', 'Karazin', 20, '2024-10-15 11:13:12'),
        (3, 'Anastasiia', 'KNU', 34, '2024-10-15 11:13:14')
    ]

    df = spark.createDataFrame(data, schema)

    stream = spark.readStream \
        .schema(schema) \
        .json(JSON_DATASETS)

    combined_df = stream.join(df, on='id', how='inner')

    outputDir = JSON_DATASETS + 'FirstTaskOutput'

    query = combined_df.writeStream \
        .outputMode('append') \
        .format('json') \
        .option('path', outputDir) \
        .option('checkpointLocation', outputDir + '\Checkpoints') \
        .start()

    query.awaitTermination()
