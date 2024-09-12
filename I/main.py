from pyspark.sql import SparkSession

import general_task as gt
import variant_task as vt


spark = SparkSession.builder.appName('Lab #1').getOrCreate()

gt.execute(spark)
vt.execute(spark)

spark.stop()
