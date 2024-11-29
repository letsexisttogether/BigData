from pyspark.sql import SparkSession
import argparse

from model import RegressorCreator

import task


parser = argparse.ArgumentParser(description='Lab #6 task divider')
parser.add_argument('--depth', required=False)
parser.add_argument('--bins', required=False)
parser.add_argument('--mipn', required=False)

args = parser.parse_args()

spark = SparkSession.builder.appName('Lab #6').getOrCreate()

depth = int(args.depth)
max_bins = int(args.bins)
min_instances_per_node = int(args.mipn)

model_creator = RegressorCreator(depth, max_bins, min_instances_per_node)

task.execute(spark, model_creator)

spark.stop()
