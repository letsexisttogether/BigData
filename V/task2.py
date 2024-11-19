from pyspark.ml.feature import VectorAssembler, \
        StandardScaler, MinMaxScaler

from general import get_df


def execute(spark):
    print('Executing task #2')

    df = get_df(spark)

    df = vectorize(df)

    standard_scale(df)
    min_max_scale(df, 0, 10)


def vectorize(df):
    assembler = VectorAssembler(inputCols=['avg'], outputCol='avg_vector')
    vectorized_df = assembler.transform(df)

    return vectorized_df


def standard_scale(df):
    scaler = StandardScaler(
        inputCol='avg_vector',
        outputCol='scaled_avg',
        withStd=True,
        withMean=True
    )

    scaled_df = scaler.fit(df).transform(df)
    scaled_df.show()


def min_max_scale(df, min, max):
    scaler = MinMaxScaler(
        inputCol='avg_vector',
        outputCol='scaled_avg_minmax',
        min=min,
        max=max
    )

    scaled_df = scaler.fit(df).transform(df)
    scaled_df.show()
