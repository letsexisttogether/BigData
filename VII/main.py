from pyspark.sql import SparkSession
import argparse

from model import ClusterCreator
import task

parser = argparse.ArgumentParser(description='Lab #6 task divider')
parser.add_argument('--k', required=False, type=int,
                    help='Number of clusters (k)')
parser.add_argument('--max_iter', required=False, type=int,
                    help='Maximum iterations')
parser.add_argument('--tol', required=False, type=float,
                    help='Tolerance')
parser.add_argument('--seed', required=False, type=int,
                    help='Seed')

args = parser.parse_args()

spark = SparkSession.builder.appName('Lab #7').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

k = args.k
max_iter = args.max_iter
tol = args.tol
seed = args.seed

model_creator = ClusterCreator(k, max_iter, tol, seed)

task.execute(spark, model_creator)

spark.stop()
