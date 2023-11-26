# Databricks notebook source
df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/mathis.da-cruz@efrei.net/products-1.csv")

# COMMAND ----------

display(df1)

# COMMAND ----------

 #df1.write.saveAsTable("products")

# COMMAND ----------

# MAGIC  %sql
# MAGIC
# MAGIC  SELECT ProductName, ListPrice
# MAGIC  FROM products
# MAGIC  WHERE Category = 'Touring Bikes';

# COMMAND ----------

dbutils.widgets.text("folder", "data")

# COMMAND ----------

folder = dbutils.widgets.get("folder")

# COMMAND ----------

print(folder)

# COMMAND ----------

path = "dbfs:/{0}/products.csv".format(folder)
dbutils.notebook.exit(path)
