from pyspark.sql import SparkSession
import argparse

import task1
import task2
import task3
import task4


parser = argparse.ArgumentParser(description='Lab #3 task divider')
parser.add_argument('--task', required=True,
                    help='Task to execute: read, letters, words')

args = parser.parse_args()

spark = SparkSession.builder.appName('Lab #3').getOrCreate()

if args.task == 'read':
    task1.execute(spark)
elif args.task == 'letters':
    task2.execute(spark)
elif args.task == 'words':
    task3.execute(spark)
elif args.task == 'text':
    task4.execute(spark)
else:
    print('There\'s no such a task option')

spark.stop()

