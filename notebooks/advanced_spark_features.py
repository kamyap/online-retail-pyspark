# Databricks notebook source
##read data from stored location
df = spark.read.format("delta").load("/tmp/retail_project/cleaned_data")


# COMMAND ----------

from pyspark.sql.functions import col, count, row_number
from pyspark.sql.window import Window

# Create a mapping of StockCode to most frequent Description
desc_window = Window.partitionBy("StockCode").orderBy(col("desc_count").desc())

desc_freq = df.groupBy("StockCode", "Description") \
    .agg(count("*").alias("desc_count"))

desc_ranked = desc_freq.withColumn("rn", row_number().over(desc_window)) \
    .filter(col("rn") == 1).drop("desc_count", "rn")

# Join back to original df
df_joined = df.join(desc_ranked, on="StockCode", how="left")
display(df_joined)

# COMMAND ----------

from pyspark.sql.functions import udf,col
from pyspark.sql.types import StringType

# Define segmentation function
def revenue_segment(revenue):
    if revenue is None:
        return "Unknown"
    elif revenue > 100:
        return "High"
    elif revenue >= 50:
        return "Medium"
    else:
        return "Low"

segment_udf = udf(revenue_segment, StringType())

# Apply UDF to add ValueSegment column
df_with_revenue = df.withColumn("Revenue", col("Quantity") * col("UnitPrice"))
df_labeled = df_with_revenue.withColumn("ValueSegment", segment_udf(col("Revenue")))

# Show count by segment
df_labeled.groupBy("ValueSegment").count().show()

# COMMAND ----------

##checking original no of partitions
df.rdd.getNumPartitions()

# COMMAND ----------

# Repartition DataFrame by Country
df_repartitioned = df.repartition("Country")
# Save to Delta table
df_repartitioned.write.format("delta").mode("overwrite").save("/mnt/retail/repartitioned_by_country")

# COMMAND ----------


####checking no of partitions after repartition
df_repartitioned.rdd.getNumPartitions()

# COMMAND ----------

# Revenue by country
from pyspark.sql.functions import sum

df_with_revenue = df.withColumn("Revenue", col("Quantity") * col("UnitPrice"))
agg_df = df_with_revenue.groupBy("Country").agg(sum("Revenue").alias("TotalRevenue"))

# Coalesce into 1 output file
agg_df.coalesce(1).write.format("delta").mode("overwrite").save("/mnt/retail/revenue_by_country_single_file")

# COMMAND ----------

