# Databricks notebook source
# MAGIC %md
# MAGIC ###Connecting to azure blob storage
# MAGIC 1. Setting spark config using azure key vault and databricks create secrets
# MAGIC 2. list the files
# MAGIC 3. read data from the demo file
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls("/"))

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("db_adls_secret_scope")

# COMMAND ----------

secret=dbutils.secrets.get(scope="db_adls_secret_scope",key="account-key1")

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.adlsstreamingpocgn2.dfs.core.windows.net",
    secret
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@adlsstreamingpocgn2.dfs.core.windows.net"))

# COMMAND ----------

df=spark.read.option("header",True).csv("abfss://demo@adlsstreamingpocgn2.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

df.display()

# COMMAND ----------


