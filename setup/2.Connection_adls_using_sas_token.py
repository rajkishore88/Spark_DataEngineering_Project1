# Databricks notebook source
# MAGIC %md
# MAGIC ###Connecting to azure blob storage
# MAGIC 1. setting the spark config for shared acess keys through azure key vault
# MAGIC 2. list file from the container
# MAGIC 3. read data from the demo file
# MAGIC
# MAGIC NOTE : SAS token has expiry timeline

# COMMAND ----------

display(dbutils.fs.ls("/"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@adlsstreamingpocgn2.dfs.core.windows.net"))


# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("db_adls_secret_scope")

# COMMAND ----------

SAS_TOKEN=dbutils.secrets.get(scope="db_adls_secret_scope",key="sas-token")
SAS_TOKEN_PROVIDER=dbutils.secrets.get(scope="db_adls_secret_scope",key="sas-token-provider")

# COMMAND ----------

print(f"{SAS_TOKEN_PROVIDER}")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.adlsstreamingpocgn2.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.adlsstreamingpocgn2.dfs.core.windows.net",SAS_TOKEN_PROVIDER)
spark.conf.set("fs.azure.sas.fixed.token.adlsstreamingpocgn2.dfs.core.windows.net",SAS_TOKEN)

# COMMAND ----------

df=spark.read.option("header",True).csv("abfss://demo@adlsstreamingpocgn2.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

df.display()

# COMMAND ----------


