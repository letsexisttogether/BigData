from names import GAMES_DATASETS, gamesSchema


def get_df(spark):
    df = spark.read.csv(GAMES_DATASETS,
                        header=True, schema=gamesSchema)

    df = df.filter(df['avg'].isNotNull())
    df = df.withColumn('avg', df['avg'].cast('double'))

    return df
