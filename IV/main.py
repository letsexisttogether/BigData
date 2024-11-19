from pyspark.sql import SparkSession
import argparse

import task1
import task2

parser = argparse.ArgumentParser(description='Lab #4 task divider')
parser.add_argument('--task', required=True,
                    help='Task to execute: first, second')

args = parser.parse_args()

spark = SparkSession.builder.appName('Lab #4').getOrCreate()

if args.task == 'first':
    task1.execute(spark)
elif args.task == 'second':
    task2.execute(spark)
else:
    print('There\'s no such a task option')

spark.stop()
