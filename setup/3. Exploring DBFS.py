# Databricks notebook source
# MAGIC %md
# MAGIC ###Trying out dbfs file system
# MAGIC 1. Enable the dbfs browser option on admin setting
# MAGIC 2. try out commands
# MAGIC 3. upload a file
# MAGIC 4. do file operation on that file 

# COMMAND ----------

display(dbutils.fs.ls("/"))

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables"))

# COMMAND ----------

display(spark.read.options(header=True,=["circuitRef","name","location"]).csv("/FileStore/tables/circuits.csv"))

# COMMAND ----------



# COMMAND ----------


