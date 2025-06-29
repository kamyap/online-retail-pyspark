# Databricks notebook source
##Read data
df = spark.read.format("delta").load("/tmp/retail_project/cleaned_data")
df.printSchema()

# COMMAND ----------

##Revenue per country
from pyspark.sql.functions import sum,round
df.groupBy("Country").agg(sum("Revenue").alias("Total_Revenue")).withColumn("Total_Revenue",round("Total_Revenue",2)).show()

# COMMAND ----------

##Top 5 products by revenue
df.groupBy("StockCode"  ,"Description").agg(sum("Revenue").alias("Revenue")).orderBy("Revenue", ascending=False).limit(5).show()

# COMMAND ----------

##monthly trends
from pyspark.sql.functions import month
from pyspark.sql.functions import to_timestamp

df = df.withColumn("InvoiceDate", to_timestamp("InvoiceDate", "dd-MM-yyyy HH:mm"))
display(df.groupBy(month("InvoiceDate").alias("Month")).agg(sum("Revenue").alias("Revenue")).orderBy("Month"))