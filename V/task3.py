from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import rand

from general import get_df


def execute(spark):
    print("Executing task #3")

    df = get_df(spark)

    df = df.orderBy(rand())

    index(df)


def index(df):
    indexer = StringIndexer(inputCol='month', outputCol='month_indexed')

    indexed_df = indexer.fit(df).transform(df)
    indexed_df.show()
