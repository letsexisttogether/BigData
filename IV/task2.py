from names import JSON_DATASETS, schema
from pyspark.sql.functions import col, to_timestamp


class StreamCreator:
    def __init__(self, spark, schema, format):
        self.spark = spark
        self.schema = schema
        self.format = format

    def create_stream(self, dir_path, watermark_time):
        stream = self.spark.readStream \
            .schema(self.schema) \
            .json(dir_path) \
            .withColumn('timestamp', to_timestamp(
                col('timestamp'), self.format)) \
            .withWatermark('timestamp', watermark_time)

        return stream


def execute(spark):
    print('Executing Task #2')

    stream_creator = StreamCreator(spark, schema, 'yyyy-MM-dd HH:mm:ss')

    first_stream = stream_creator.create_stream(JSON_DATASETS + 'First/',
                                                '5 minutes')
    second_stream = stream_creator.create_stream(JSON_DATASETS + 'Second/',
                                                 '1 minutes')

    combined_stream = first_stream.join(second_stream, on='id', how='inner')

    outputDir = JSON_DATASETS + 'SecondTaskOutput'

    query = combined_stream.coalesce(1).writeStream \
        .outputMode('append') \
        .format('json') \
        .option('path', outputDir) \
        .option('checkpointLocation', outputDir + '/Checkpoints') \
        .trigger(processingTime='10 seconds')\
        .start()

    query.awaitTermination()
