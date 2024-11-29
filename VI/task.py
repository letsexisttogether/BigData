from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.evaluation import RegressionEvaluator

from general import get_df

from model import RegressorCreator


def execute(spark, creator: RegressorCreator):
    print('Executing task with multiple models')

    in_features = ['avg', 'gain']
    out_featues = 'features'
    target = 'peak'
    prediction = 'prediction'

    df = get_df(spark, [*in_features, target])

    assembler = VectorAssembler(
        inputCols=in_features,
        outputCol=out_featues
    )
    df = assembler.transform(df)

    df = scale_features(df, out_featues)

    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

    regressor = creator.get_regressor(out_featues, target, prediction)
    model = regressor.fit(train_df)

    predicted_df = model.transform(test_df)

    print('Final DataFrame:')
    predicted_df.select(out_featues, target, prediction).show(10)

    evaluate_model(predicted_df, target, prediction, 'r2', 'R²')
    evaluate_model(predicted_df, target, prediction, 'rmse', 'RMSE')


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


def evaluate_model(predicted_df, target, prediction, metric, metric_name):
    evaluator = RegressionEvaluator(
        labelCol=target,
        predictionCol=prediction,
        metricName=metric
    )

    metric_result = evaluator.evaluate(predicted_df)
    print(f'{metric_name}: {metric_result}')
