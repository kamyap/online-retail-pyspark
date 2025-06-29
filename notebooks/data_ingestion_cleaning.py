# Databricks notebook source
#Read data from csv & add revenue column
df = spark.read.option("header", True).csv("/FileStore/tables/Online_Retail.csv", inferSchema=True)
df=df.withColumn("Revenue",col('UnitPrice')*col('Quantity'))

# COMMAND ----------

##save data in location
df.write.mode("overwrite").format("delta").save("/tmp/retail_project/cleaned_data")

# COMMAND ----------

##Checking on storage of data
display(dbutils.fs.ls("/tmp/retail_project/cleaned_data"))

# COMMAND ----------

