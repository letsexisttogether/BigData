from pyspark.ml.feature import Bucketizer, QuantileDiscretizer

from general import get_df


def execute(spark):
    print('Executing task #1')

    df = get_df(spark)

    max_avg_val = df.agg(max('avg').alias('max')).collect()[0]['max']
    split_range = [0, 10000, 20000, 50000, 100000, max_avg_val]

    manual_split(df, split_range)
    automatical_split(df, 4)


def manual_split(df, splits):
    bucketizer = Bucketizer(
        splits=splits,
        inputCol='avg',
        outputCol='bucketized_avg'
    )

    bucketized_df = bucketizer.transform(df)
    bucketized_df.show()


def automatical_split(df, buckets_num):
    discretizer = QuantileDiscretizer(
        numBuckets=buckets_num,
        inputCol='avg',
        outputCol='quantiled_avg',
    )

    quantiled_model = discretizer.fit(df)

    splits = quantiled_model.getOrDefault('splits')

    print('Automatically calculated splits:')
    for i, split in enumerate(splits):
        print(f'Bucket {i}: {split}')

    quantiled_df = quantiled_model.transform(df)
    quantiled_df.show()
