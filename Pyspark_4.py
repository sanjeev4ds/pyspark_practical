from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import monotonically_increasing_id, col

spark = SparkSession.builder.appName("lecture4").getOrCreate()

# without header, without schema,
# and also considering first row of csv as record
df = spark.read\
    .format("csv")\
    .option("header", "false")\
    .option("inferschema", "false")\
    .load("flight_data_2010_summary.csv")
df.show(5)
df.printSchema()

#creating schema for flight data
schema_flight_data = StructType([
    StructField("DEST_COUNTRY_NAME", StringType(), False),
    StructField("ORIGIN_COUNTRY_NAME", StringType(), False),
    StructField("count", IntegerType(), False)
])

# header false, with schema but not inferred,
# and also considering first row of csv as record
df_with_schema = spark.read\
    .format("csv")\
    .option("header", "false")\
    .option("inferschema", "false")\
    .schema(schema_flight_data)\
    .load("light_data_2010_summary.csv")
df_with_schema.show(5)
df_with_schema.printSchema()

#creating ddl schema for flight data
schema_flight_data_ddl = """DEST_COUNTRY_NAME string not null, 
    ORIGIN_COUNTRY_NAME string not null,
    count integer not null"""

# without header, with schema_ddl
# and also considering first row of csv as record
df_with_schema_ddl = spark.read\
    .format("csv")\
    .option("header", "false")\
    .option("inferschema", "false")\
    .schema(schema_flight_data_ddl)\
    .load("flight_data_2010_summary.csv")
df_with_schema_ddl.show(5)
df_with_schema_ddl.printSchema()


# header true to prevent first row of csv to be included in data 
# and also with schema but not inferred
# and also skipping first row of csv with option("skipRows", 1) may work in databricks and specific environment only
#skipRows failed in my local setup
df_with_schema_and_header = spark.read\
    .format("csv")\
    .option("header", "true")\
    .option("inferschema", "false")\
    .schema(schema_flight_data)\
    .load("flight_data_2010_summary.csv")
df_with_schema_and_header.show(5)
df_with_schema_and_header.printSchema()

#removing first row
number_of_rows_to_skip = 1
#df_with_schema having header row from csv file included in its Dataframe first row
df_with_schema_skipped_first_row = df_with_schema.withColumn(
    "row_id", monotonically_increasing_id()
)
#as monotonically_increasing_id starts from 0
df_with_schema_skipped_first_row.filter(col("row_id") >= number_of_rows_to_skip)
df_with_schema_skipped_first_row.show(5)

print("press enter to process")
input()
spark.stop()
print("end successfully")
