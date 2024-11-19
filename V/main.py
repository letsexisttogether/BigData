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

if args.task == '1':
    task1.execute(spark)
elif args.task == '2':
    task2.execute(spark)
elif args.task == '3':
    task3.execute(spark)
elif args.task == '4':
    task4.execute(spark)
else:
    print('There\'s no such a task option')

spark.stop()