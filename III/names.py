from pyspark.sql.types import StructType, StructField, IntegerType, \
    StringType, FloatType

DATASETS = 'D:/RegretALot/Subjects/BigData/Datasets/'
VOTES_DATASETS = DATASETS + 'Votes/'
GAMES_DATASETS = DATASETS + 'Games/'
TEXT_DATASETS = DATASETS + 'Text/'

unvotesSchema = StructType([
    StructField('rcid', IntegerType(), False),
    StructField('country', StringType(), False),
    StructField('country_code', StringType(), False),
    StructField('vote', StringType(), False)
])

gamesSchema = StructType([
    StructField('gamename', StringType(), False),
    StructField('year', IntegerType(), False),
    StructField('month', StringType(), False),
    StructField('avg', FloatType(), False),
    StructField('gain', FloatType(), False),
    StructField('peak', StringType(), False),
    StructField('avg_peak_perc', StringType(), False)
])
