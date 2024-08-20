from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType

spark = SparkSession.builder.appName("lecture10").getOrCreate()

data_df = [(1,1), (2,1), (3,1), (4,2), (5,1), (6,2), (7,2)]
schema_df = StructType([
    StructField("id", IntegerType(), False),
    StructField("num", IntegerType(), False)
])
df = spark.createDataFrame(data=data_df, schema=schema_df)
df.show()
