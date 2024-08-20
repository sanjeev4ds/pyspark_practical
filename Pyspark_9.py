from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("lecture9")\
    .config("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true")\
    .getOrCreate()

df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferscehma", "true")\
    .load("data_for_lectures/sample_data_for_parquet.csv")

df.show()

#partition by address

df.write.format("parquet")\
    .option("header", "true")\
    .option("mode", "overwrite")\
    .partitionBy("address", "gender")\
    .option("path", "data_for_lectures/partition/partition_by_address")\
    .save()

#partition by address and further by gender
df.write.format("parquet")\
    .option("header", "true")\
    .option("mode", "overwrite")\
    .partitionBy("address", "gender")\
    .option("path", "data_for_lectures/partition/partition_by_address_gender")\
    .save()

#bucket by id and 3 buckets
df.write.format("csv")\
    .option("header", "true")\
    .option("path", "data_for_lectures/bucketing/bucket_by_id")\
    .bucketBy(3, "id")\
    .saveAsTable("bucket_by_id_table")
#unable to save tables in local setup

print("press enter to process")
input()
spark.stop()
print("end successfully")
