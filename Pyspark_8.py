from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("lecture8").getOrCreate()

df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferschema", "true")\
    .load("data_for_lectures/sample_data_for_parquet.csv")

df.write.format("csv")\
    .option("header", "true")\
    .option("mode", "ignore")\
    .option("path", "data_for_lectures/save_files/")\
    .save()

df.repartition(3).write.format("parquet")\
    .option("header", "true")\
    .option("mode", "overwrite")\
    .option("path", "data_for_lectures/save_files_repartition1/")\
    .save()

print("press enter to process")
input()
spark.stop()
print("end successfully")
