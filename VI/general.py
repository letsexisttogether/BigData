from names import GAMES_DATASETS, gamesSchema
from typing import List


def get_df(spark, columns: List[str]):
    df = spark.read.csv(GAMES_DATASETS,
                        header=True, schema=gamesSchema)

    for column in columns:
        if column not in df.columns:
            raise ValueError(f'Column {column} does not \
                exist in the DataFrame')

        df = df.filter(df[column].isNotNull()) \
            .withColumn(column, df[column].cast('double'))

    return df
