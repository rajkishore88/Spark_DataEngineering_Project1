# Databricks notebook source
# MAGIC %md
# MAGIC ###Creating the connection to Raw Container of the Azure Data Factory gen2 Storage using acess key
# MAGIC 1. Create the access key for demo and store in the Azure Key Vault
# MAGIC 2. Configure the spark config to access the demo container 
# MAGIC 3. Read a sample file into spak data frame using reader API

# COMMAND ----------

display(dbutils.fs.ls("/"))

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

display(dbutils.secrets.listScopes())

# COMMAND ----------

display(dbutils.secrets.list("db_adls_secret_scope"))

# COMMAND ----------

Secret_to_connect=dbutils.secrets.get(scope="db_adls_secret_scope",key="account-key1")

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.adlsstreamingpocgn2.dfs.core.windows.net",
    Secret_to_connect
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@adlsstreamingpocgn2.dfs.core.windows.net"))

# COMMAND ----------

race_df=spark.read.options(header=True).csv("abfss://raw@adlsstreamingpocgn2.dfs.core.windows.net/races.csv")
display(race_df)

# COMMAND ----------


