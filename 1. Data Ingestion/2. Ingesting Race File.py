# Databricks notebook source
# MAGIC %md
# MAGIC ### Setting up the Environment

# COMMAND ----------

# MAGIC %run "/Repos/Raj/Spark_DataEngineering_Project1/setup/Setup Script"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Reading Race File to Data Frame

# COMMAND ----------

race_df=spark.read.options(header=True).csv(f"{raw_folder_path}/races.csv")
display(race_df)

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType,TimestampType,DateType
race_schema=StructType([StructField("race_id",IntegerType(),False),
                       StructField("race_year",IntegerType(),False),
                       StructField("round",IntegerType(),False),
                       StructField("circuit_id",IntegerType(),False),
                       StructField("name",StringType(),True),
                       StructField("date",DateType(),True),
                       StructField("time",StringType(),True),
                       StructField("url",StringType(),True)
                      ])


# COMMAND ----------

race_df=spark.read.csv(f"{raw_folder_path}/races.csv",schema=race_schema,header=True)
race_df.display()

# COMMAND ----------

from pyspark.sql.functions import col,concat,to_timestamp,lit,current_timestamp
race_df1=race_df\
    .withColumn("race_timestamp",to_timestamp(concat(race_df.date,lit(' '),race_df.time),'yyyy-MM-dd hh:mm:ss'))\
    .withColumn("ingestion_date",current_timestamp())\
        .drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write the Dataframe To Processed Folder in Blob Storage in Parquet Format

# COMMAND ----------

race_df1.write.mode("overwrite").partitionBy("race_year").parquet(f"{processed_folder_path}/race")

# COMMAND ----------

display(dbutils.fs.ls(f"{processed_folder_path}/race"))
