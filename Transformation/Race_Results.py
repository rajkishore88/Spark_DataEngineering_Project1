# Databricks notebook source
# MAGIC %run "/Repos/Raj/Spark_DataEngineering_Project1/setup/Setup Script"

# COMMAND ----------

display(dbutils.fs.ls(processed_folder_path))

# COMMAND ----------

drivers_df=spark.read.parquet(f"{processed_folder_path}/drivers")\
    .withColumnRenamed("fullname","driver_name")\
    .withColumnRenamed("number","driver_number")\
    .withColumnRenamed("nationality","driver_nationality")\
    .withColumnRenamed("driver_Id","driver_id")

# COMMAND ----------

constructor_df=spark.read.parquet(f"{processed_folder_path}/constructor")\
.withColumnRenamed("name","Team")\
.withColumnRenamed("constructor_Id","constructor_id")

# COMMAND ----------

circuits_df=spark.read.parquet(f"{processed_folder_path}/circuits")\
    .withColumnRenamed("name","circuits_name")\
    .withColumnRenamed("location","circuit_location")

# COMMAND ----------

results_df=spark.read.parquet(f"{processed_folder_path}/results")\
    .withColumnRenamed("time","race_time")

# COMMAND ----------

race_df=spark.read.parquet(f"{processed_folder_path}/race")\
    .withColumnRenamed("name","race_name")\
    .withColumnRenamed("timestamp","race_timestamp")\
    .withColumnRenamed("date","race_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Join Circuits to Race

# COMMAND ----------

race_circuits_df=race_df.join(circuits_df,race_df.circuit_id==circuits_df.circuit_id,"inner")\
.select(race_df.race_id,race_df.race_year,race_df.race_name,race_df.race_date,circuits_df.circuit_location)

# COMMAND ----------

race_results_df=results_df.join(race_circuits_df,results_df.race_id==race_circuits_df.race_id,"inner")\
                          .join(drivers_df,results_df.driver_id==drivers_df.driver_id,"inner")\
                          .join(constructor_df,results_df.constructor_id==constructor_df.constructor_id,"inner")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

final_df=race_results_df.select("race_year","race_name","race_date","circuit_location","driver_name","driver_number","driver_nationality","team","grid","fastest_lap","race_time","points")\
    .withColumn("created_date",current_timestamp())

# COMMAND ----------

final_df.display()

# COMMAND ----------

final_df.filter("race_year == '2020' and race_name=='Abu Dhabi Grand Prix'").sort(final_df.points.desc()).display()

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(dbutils.fs.ls(f"{presentation_folder_path}/race_results"))

# COMMAND ----------


