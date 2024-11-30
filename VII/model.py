from pyspark.ml.clustering import KMeans


class ClusterCreator:
    k_default = 2
    max_iter_default = 20
    tol_default = 1e-4
    seed_default = 42

    def __init__(self, k, max_iter, tol, seed):
        self.k = k if k is not None \
            else ClusterCreator.k_default
        self.max_iter = max_iter if max_iter is not None \
            else ClusterCreator.max_iter_default
        self.tol = tol if tol is not None \
            else ClusterCreator.tol_default
        self.seed = seed if seed is not None \
            else ClusterCreator.seed_default

    def get_clustering_model(self, features_col, cluster_col):
        kmeans = KMeans(
            featuresCol=features_col,
            predictionCol=cluster_col,
            k=self.k,
            maxIter=self.max_iter,
            tol=self.tol,
            seed=self.seed
        )

        print(
            'The model params:',
            f'Number of clusters (k): {self.k}',
            f'Max iterations: {self.max_iter}',
            f'Tolerance: {self.tol}',
            sep='\n'
        )

        return kmeans
