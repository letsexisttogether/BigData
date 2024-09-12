from pyspark.sql import Row
from pyspark.sql.functions import lit


def execute(spark):
    print('Executing general task...\n')

    pets_data = [
        Row(ID=1, Name='Buddy', Age=5, Type='Dog'),
        Row(ID=2, Name='Mittens', Age=3, Type='Cat'),
        Row(ID=3, Name='Rex', Age=4, Type='Dog'),
        Row(ID=4, Name='Whiskers', Age=2, Type='Cat'),
        Row(ID=5, Name='Rover', Age=6, Type='Dog')
    ]
    pets_df = spark.createDataFrame(pets_data)

    print('Pets DataFrame:')
    pets_df.show()

    owners_data = [
        ['Alice', 1, 30, 70000],
        ['Bob', 2, 25, 50000],
        ['Charlie', 3, 35, 80000],
        ['Diana', 4, 28, 60000],
        ['Edward', 5, 40, 75000]
    ]
    owners_schema = 'Name STRING, PetID INT, Age INT, Income INT'
    owners_df = spark.createDataFrame(owners_data, schema=owners_schema)

    print('Owners DataFrame:')
    owners_df.show()

    new_pets_data = [
        Row(ID=6, Name='Sparky', Age=2, Type='Dog'),
        Row(ID=7, Name='Fluffy', Age=1, Type='Cat')
    ]
    new_pets_df = spark.createDataFrame(new_pets_data)
    pets_df = pets_df.union(new_pets_df)

    pets_df = pets_df.withColumn('IsVaccinated', lit('yes'))

    print('Pets DataFrame with IsVaccinated:')
    pets_df.show()

    joined_df = pets_df.join(owners_df, pets_df.ID == owners_df.PetID,
                             'inner').drop(owners_df.PetID)

    print('Joined DataFrame:')
    joined_df.show()
