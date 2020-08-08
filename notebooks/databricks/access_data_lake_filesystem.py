# Databricks notebook source
# MAGIC %md
# MAGIC # Mounting azure data lake on the cluster using access keys and databricks secrets
# MAGIC 
# MAGIC We can use this notebook to setup access to the the data lake storage from databricks directly.
# MAGIC Please follow first the steps in the powerpoint presentation ("create an azure data lake" and "acess the filesystem from DB", in sequence).
# MAGIC Then start a cluster, attach this notebook to the cluster, and run the following chunks, replacing the parts between <> with the values you
# MAGIC setup while following the presentation instructions.

# COMMAND ----------

# retrieve the access key from the databricks secret vault. <scope> and <key> are the values you setup while following the steps in the presentation
spark.conf.set("fs.azure.account.key.adsliot.dfs.core.windows.net", dbutils.secrets.get(scope = <scope>, key = <key>)) # this gets the access-key of your datalake from the databricks keyvault

# COMMAND ----------

# Example:
spark.conf.set("fs.azure.account.key.adsliot.dfs.core.windows.net", dbutils.secrets.get(scope="dataconnections", key="connection"))

# COMMAND ----------

# we can now list the folders in your data lake
dbutils.fs.ls("abfss://<your-storage-account-name>@<your-container-name>.dfs.core.windows.net")

# COMMAND ----------

# in my case:
dbutils.fs.ls("abfss://iotdata@adsliot.dfs.core.windows.net")

# COMMAND ----------

# MAGIC %md
# MAGIC Even nicer, with the fs magic

# COMMAND ----------

# MAGIC %fs ls "abfss://iotdata@adsliot.dfs.core.windows.net"

# COMMAND ----------

# we can check out the files
%fs ls "abfss://iotdata@adsliot.dfs.core.windows.net/Raw"

# COMMAND ----------

import os
BASE_FOLDER = "abfss://iotdata@adsliot.dfs.core.windows.net"
RAW_FOLDER = os.path.join(BASE_FOLDER, "Raw")
CLEAN_FOLDER = os.path.join(BASE_FOLDER, "Clean")

# COMMAND ----------

# MAGIC %md
# MAGIC Unfortunately, without mounting the data lake on DBFS this won't work:

# COMMAND ----------

# MAGIC %fs ls BASE_FOLDER

# COMMAND ----------

# MAGIC %md
# MAGIC But this does

# COMMAND ----------

display(dbutils.fs.ls(RAW_FOLDER))