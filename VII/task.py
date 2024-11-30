from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.evaluation import ClusteringEvaluator

from general import get_df
from model import ClusterCreator


def execute(spark, creator: ClusterCreator):
    print('Executing task about clustering')

    in_features = ['avg', 'peak']
    out_features = 'features'
    cluster_col = 'cluster'

    df = get_df(spark, in_features)

    assembler = VectorAssembler(
        inputCols=in_features,
        outputCol=out_features
    )
    df = assembler.transform(df)

    # df = scale_features(df, out_featues)

    kmeans = creator.get_clustering_model(out_features, cluster_col)
    model = kmeans.fit(df)

    clustered_df = model.transform(df)

    '''
    print('Clustered DataFrame:')
    clustered_df.drop('features').show(10)

    print('Cluster Centers:')
    for i, center in enumerate(model.clusterCenters()):
        print(f'Cluster {i}: {center}')
    '''

    evaluate_model(clustered_df, out_features, cluster_col)


def scale_features(df, column_name):
    scaled_column_name = column_name + '_scaled'

    scaler = StandardScaler(
        inputCol=column_name,
        outputCol=scaled_column_name,
        withMean=True,
        withStd=True
    )
    scaler_model = scaler.fit(df)
    scaled_df = scaler_model.transform(df)

    result_df = scaled_df.drop(column_name) \
        .withColumnRenamed(scaled_column_name, column_name)

    return result_df


def evaluate_model(clustered_df, features_col, prediction_col):
    evaluator = ClusteringEvaluator(
        featuresCol=features_col,
        predictionCol=prediction_col,
        metricName='silhouette',
        distanceMeasure='squaredEuclidean'
    )

    silhouette = evaluator.evaluate(clustered_df)
    print(f'Silhouette Score: {silhouette}')
