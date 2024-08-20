from pyspark.sql import SparkSession
import pyarrow as pa
import pyarrow.parquet as pq

parquet_file = pq.ParquetFile('part-r-00000-1a9822ba-b8fb-4d8e-844a-ea30d0801b9e.gz.parquet')

print("parquet_file.metadata")
print(parquet_file.metadata)

print("\n\n")
print("parquet_file.metadata.row_group(0)")
print("***************************************")
print(parquet_file.metadata.row_group(0))

print("\n\n")
print("parquet_file.metadata.row_group(0).column(0)")
print("***************************************")
print(parquet_file.metadata.row_group(0).column(0))


print("\n\n")
print("parquet_file.metadata.row_group(0).column(0).statistics")
print("***************************************")
print(parquet_file.metadata.row_group(0).column(0).statistics)

print("\n\n")
print("parquet_file.metadata.row_group(0).column(1).statistics")
print("***************************************")
print(parquet_file.metadata.row_group(0).column(1).statistics)

print("\n\n")
print("parquet_file.metadata.row_group(0).column(2).statistics")
print("***************************************")
print(parquet_file.metadata.row_group(0).column(2).statistics)


spark = SparkSession.builder.appName("lecture7").getOrCreate()

df = spark.read.parquet("data_for_lectures/part-r-00000-1a9822ba-b8fb-4d8e-844a-ea30d0801b9e.gz.parquet")
df.show()

print("press enter to process")
input()
spark.stop()
print("end successfully")
