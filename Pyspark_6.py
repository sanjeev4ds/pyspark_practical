from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("lecture6").getOrCreate()

#line delimited json with only 3 keys at each record
df_line_delimited = spark.read.format("json")\
    .option("inferschema", "true")\
    .option("mode", "PERMISSIVE")\
    .load("data_for_lectures/line_delimited_json.json")
df_line_delimited.show()

#line delimited but have 1 extra key in one of the records
df_line_delimited_extra_field = spark.read.format("json")\
    .option("inferschema", "true")\
    .option("mode", "PERMISSIVE")\
    .load("data_for_lectures/single_file_json_with_extra_fields.json")
df_line_delimited_extra_field.show()

#line delimited but have one of the records not closed
df_line_delimited_record_not_closed = spark.read.format("json")\
    .option("inferschema", "true")\
    .option("mode", "PERMISSIVE")\
    .load("data_for_lectures/corrupted_json.json")
df_line_delimited_record_not_closed.show()

#multi line json
df_multi_line = spark.read.format("json")\
    .option("inferschema", "true")\
    .option("mode", "PERMISSIVE")\
    .option("multiline","true")\
    .load("data_for_lectures/Multi_line_correct.json")
df_multi_line.show()

#multi line json but incorrect
df_multi_line_incorrect = spark.read.format("json")\
    .option("inferschema", "true")\
    .option("mode", "PERMISSIVE")\
    .option("multiline", "true")\
    .load("data_for_lectures/Multi_line_incorrect.json")
df_multi_line_incorrect.show()
