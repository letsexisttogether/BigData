from pyspark.sql import SparkSession

import variant_task as vq

spark = SparkSession.builder.appName('Lab #2').getOrCreate()

vq.execute(spark)

spark.stop()
