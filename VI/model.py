from pyspark.ml.regression import DecisionTreeRegressor


class RegressorCreator:
    depth_default = 5
    max_bins_default = 32
    min_instances_per_node_default = 1

    def __init__(self, depth, max_bins, mipn):
        self.depth = depth if depth is not None \
            else RegressorCreator.depth_default
        self.max_bins = max_bins if max_bins is not None \
            else RegressorCreator.max_bins_default
        self.min_instances_per_node = mipn if mipn is not None \
            else RegressorCreator.min_instances_per_node_default

    def get_regressor(self, features, target, prediction):
        regressor = DecisionTreeRegressor(
            featuresCol=features,
            labelCol=target,
            predictionCol=prediction,
            maxDepth=self.depth,
            maxBins=self.max_bins,
            minInstancesPerNode=self.min_instances_per_node
        )

        print('The model params:', f'Depth: {self.depth}',
              f'Max bins: {self.max_bins}',
              f'Min instancer per node: {self.min_instances_per_node}',
              sep='\n')

        return regressor
