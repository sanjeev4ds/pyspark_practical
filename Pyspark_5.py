from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
spark = SparkSession.builder.appName("lecture5").getOrCreate()

df_with_permissive = spark.read.format("csv")\
    .option('header', 'true')\
    .option('inferschema', 'true')\
    .option('mode', 'PERMISSIVE')\
    .load('data_corrupted_records.csv')
df_with_permissive.show()

df_with_dropmalformed = spark.read.format("csv")\
    .option('header', 'true')\
    .option('inferschema', 'true')\
    .option('mode', 'DROPMALFORMED')\
    .load('data_for_lectures/data_corrupted_records.csv')
df_with_dropmalformed.show()

schema_df = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("age",IntegerType(),False),
    StructField("salary",IntegerType(),False),
    StructField("address", StringType(), False),
    StructField("nominee", StringType(), False),
    StructField("_corrupt_record", StringType(), True)
])

df_with_failfast = spark.read.format("csv")\
    .option('header', 'true')\
    .option('inferschema', 'true')\
    .schema(schema_df)\
    .option('mode', 'FAILFAST')\
    .option("badRecordsPath", "BadRecords")\
    .load('data_for_lectures/data_corrupted_records.csv')

df_with_failfast.show()

print("press enter to process")
input()
spark.stop()
print("end successfully")
