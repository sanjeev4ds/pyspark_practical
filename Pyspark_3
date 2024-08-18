from pyspark.sql import SparkSession
from pyspark import SparkFiles

spark = SparkSession.builder.appName("lecture3").getOrCreate()

# # Set log level
# spark.sparkContext.setLogLevel("INFO")

flight_data_github_raw_url = "https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/master/data/flight-data/csv/2010-summary.csv"
spark.sparkContext.addFile(flight_data_github_raw_url)

#mode are of 3 types- FAILFAST, DROPMALFORMED, PERMISSIVE

#reading file from SparkFiles
#df with format csv, header true and inferschema true and mode FAILFAST
df = spark.read\
    .format("csv")\
    .option("header", "true")\
    .option("inferschema", "true")\
    .option("mode", "FAILFAST")\
    .load(SparkFiles.get("2010-summary.csv"))
df.show(5)
df.printSchema()

#reading file from absolute path
#df with format csv, header false and inferschema false and mode FAILFAST
df_nonheader = spark.read\
    .format("csv")\
    .option("header", "false")\
    .option("inferschema", "false")\
    .option("mode", "PERMISSIVE")\
    .load("flight_data_2010_summary.csv")
df_nonheader.show(5)
df_nonheader.printSchema()

print("please enter to further stop the process", input())
spark.stop()
print("finally stopped")
