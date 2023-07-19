# Databricks notebook source
# MAGIC %run ../setup/4.Connection_to_raw_Container

# COMMAND ----------

# MAGIC %run "../Includes/1. Storage path"

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Step 1 - Read the JSON file using spark dataframe read API

# COMMAND ----------

dbutils.fs.ls(f"{raw_folder_path}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Declaring the Schema for Drivers File

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DateType
name_schema=StructType([StructField("forename",StringType(),True),
                               StructField("surname",StringType(),True)])


driver_schema=StructType([StructField("code",StringType(),True),
                                 StructField("dob",DateType(),True),
                                 StructField("driverId",StringType(),True),
                                 StructField("name", name_schema),
                                 StructField("nationality",StringType(),True),
                                 StructField("number",IntegerType(),True),
                                 StructField("url",StringType(),True)
                                 ])

# COMMAND ----------

####Reading The File into the DataFrame using the Schema

# COMMAND ----------

driver_df=spark.read.schema(driver_schema).json(f"{raw_folder_path}/drivers.json")
driver_df.display()


# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ###Transform the required Columns
# MAGIC 1. driverId rename to driver_Id
# MAGIC 2. driverRef rename to driver_Ref
# MAGIC 3. ingestion date added
# MAGIC 4. name added with concatenation of forename and surname
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,concat,col,lit
#from pyspark.sql import withColumnRenamed,withColumn

driver_df_edit=driver_df.withColumnRenamed("driverId","driver_Id")\
    .withColumnRenamed("code","driver_Ref")\
    .withColumn("ingestion_date",current_timestamp())\
    .withColumn("fullname",concat(col("name.forename"),lit(" "),col("name.surname")))

# COMMAND ----------

driver_df_final=driver_df_edit.drop("url")\
    .drop("name")
driver_df_final.display()

# COMMAND ----------

driver_df_final.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers")

# COMMAND ----------


