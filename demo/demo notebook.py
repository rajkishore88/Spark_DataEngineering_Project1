# Databricks notebook source
# MAGIC %run "/Repos/Raj/Spark_DataEngineering_Project1/setup/Setup Script"

# COMMAND ----------

display(dbutils.fs.ls(processed_folder_path))

# COMMAND ----------

display(dbutils.fs.ls(f"{processed_folder_path}/race"))

# COMMAND ----------

races_df=spark.read.parquet(f"{processed_folder_path}/race")

# COMMAND ----------

races_df.display()

# COMMAND ----------

races_filter_df=races_df.filter((races_df["race_year"]==2019) & (races_df["round"]==2))
races_filter_df.display()

# COMMAND ----------

drivers_df=spark.read.parquet(f"{processed_folder_path}/drivers")

# COMMAND ----------

results_df=spark.read.parquet(f"{processed_folder_path}/results")
results_df.display()

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

drivers_df.display()

# COMMAND ----------

results_with_driver_detail=results_df.join(drivers_df,"driver_id","inner")

# COMMAND ----------

results_with_driver_detail.drop("fastest_lap","fastest_lap_speed","fastest_lap_time","rank","ingestion_date","dob")

# COMMAND ----------

results_with_driver_detail.display()

# COMMAND ----------


