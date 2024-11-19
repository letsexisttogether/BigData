from pyspark.sql.types import StructType, StructField, IntegerType, StringType

DATASETS = 'D:/RegretALot/Subjects/BigData/Datasets/'
JSON_DATASETS = DATASETS + 'Json/'

schema = StructType([
    StructField('id', IntegerType(), False),
    StructField('name', StringType(), False),
    StructField('university', StringType(), False),
    StructField('top', IntegerType(), False),
    StructField('timestamp', StringType(), False)
])
