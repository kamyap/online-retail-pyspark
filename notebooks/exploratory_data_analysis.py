# Databricks notebook source
##read data from stored location
df = spark.read.format("delta").load("/tmp/retail_project/cleaned_data")
df.printSchema()

# COMMAND ----------

##show total count
display(df.count())

# COMMAND ----------

##displaying numeric stats
df.select("Quantity", "UnitPrice", "Revenue").describe().show()

# COMMAND ----------

##Sisplaying Null count
df=df.filter(df["CustomerID"].isNull())
display(df.agg(count("*").alias("count")))

display(df.select("CustomerID","Country","StockCode").distinct())

# COMMAND ----------

##Distinct values for columns mentioned
display(df.select("CustomerID","Country","StockCode").distinct())

# COMMAND ----------

#Distinct Count of transactions
display(df.select("InvoiceNo").distinct().count())

# COMMAND ----------

##show grouped count for dataset
display(df.groupBy("Country").count())
