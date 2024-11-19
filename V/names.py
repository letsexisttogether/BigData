from pyspark.sql.types import StructType, StructField, IntegerType, \
    StringType, FloatType, DoubleType

DATASETS = 'D:/RegretALot/Subjects/BigData/Datasets/'
GAMES_DATASETS = DATASETS + 'Games/sliced-tidytuesday.csv'

gamesSchema = StructType([
    StructField('gamename', StringType(), False),
    StructField('year', IntegerType(), False),
    StructField('month', StringType(), False),
    StructField('avg', DoubleType(), False),
    StructField('gain', FloatType(), False),
    StructField('peak', StringType(), False),
    StructField('avg_peak_perc', StringType(), False)
])
