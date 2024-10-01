from pyspark.sql.types import StructType, StructField, StringType, IntegerType


DATASETS = 'D:/RegretALot/Subjects/BigData/Datasets/'


def execute(spark):
    print('Executing variant #7 task...\n')

    schema = StructType([
        StructField('Age', IntegerType(), False),
        StructField('Gender', StringType(), False),
        StructField('Region', StringType(), False),
        StructField('Occupation', StringType(), False),
        StructField('Income', IntegerType(), False),
        StructField('Has Laptop', StringType(), False),
    ])

    df = spark.read.csv(DATASETS + 'Laptop-Users.csv',
                        header=True, schema=schema)
    df.createOrReplaceTempView('LaptopUsers')

    task_1(spark)
    task_2(spark, 3)
    task_3(spark)
    table = task_4(spark)

    # save_table(table)


def task_1(spark):
    table = spark.sql('SELECT Occupation, COLLECT_LIST(Age) as Ages \
        FROM LaptopUsers GROUP BY Occupation')

    table.show()

    table.createOrReplaceTempView('GroupedLaptopUsers')


def task_2(spark, max_ages_count):
    columns = ['Occupation']

    columns += [f'Ages[{i}] AS Age_{i + 1}' for i in range(max_ages_count)]

    query = f'SELECT {", ".join(columns)} FROM GroupedLaptopUsers'
    print(query)

    table = spark.sql(query)

    table.show()


def task_3(spark):
    table = spark.sql('SELECT Gender, Region, AVG(Age) AS AverageAge \
            FROM LaptopUsers GROUP BY Gender, Region')

    table.show()


def task_4(spark):
    table = spark.sql('SELECT Occupation, AVG(Income) AS Average_income \
        FROM LaptopUsers GROUP BY Occupation')

    table.createOrReplaceTempView('AverageIncomes')

    table = spark.sql('SELECT lpt.*, (avg.Average_income - lpt.Income) \
        AS IncomeDiff FROM LaptopUsers lpt \
        JOIN AverageIncomes avg ON lpt.Occupation = avg.Occupation')

    table.createOrReplaceTempView('LaptopUsers')

    table = spark.sql('SELECT * FROM LaptopUsers')

    table.show()

    return table


def save_table(table):
    table.write.mode('overwrite') \
        .csv(DATASETS + 'Laptop-Users-lab#2.csv', header=True)

    print('The file has been successfully written')
