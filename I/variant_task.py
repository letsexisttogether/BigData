from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyspark.sql.functions as fn


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

    calculate_average_income(df)
    select_countryside_women_income(df)
    count_no_laptop_male_students(df)
    sort_laptop_users_by_age(df)


def calculate_average_income(df):
    average_income_df = df.where(fn.col('Occupation') == 'banker') \
        .agg(fn.avg('Income').alias('Average income'))
    average_income_df.show()


def select_countryside_women_income(df):
    countryside_women_income = df.where(
        (fn.col('Region') == 'countryside') & (fn.col('Gender') == 'female')) \
        .select(fn.col('Income').alias('Countryside women income'))

    countryside_women_income.show()


def count_no_laptop_male_students(df):
    no_latop_male_students_count = df.where(
        (fn.col('Gender') == 'male') & (fn.col('Occupation') == 'student')
        & (fn.col('Has Laptop') == 'no')).count()

    print('The amount of male students who does not have a laptop is',
          no_latop_male_students_count, '\n')


def sort_laptop_users_by_age(df):
    sorted_laptop_users = df.where(fn.col('Has Laptop') == 'yes') \
        .orderBy('Age', ascending=False)

    sorted_laptop_users.show()
