# Databricks notebook source
# Define the connection properties
jdbc_url = "jdbc:sqlserver://sql-server-lab2.database.windows.net:1433;database=sql-bdd-lab2"
connection_properties = {
    "user": "mathis",
    "password": "XXXXXXXX",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}
 
# Define the table name
table_name = "stocks"
 
# Read the table into a DataFrame
df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
 
# Show the DataFrame
df.show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def calculate_average_daily_return(df, stock_name, start_date, end_date):
    df_filtered_date_stock = df.filter((F.col('Date').between(start_date, end_date)) & (F.col('stock_name') == stock_name))
    daily_return_rate_df =  df_filtered_date_stock.withColumn("daily_return_rate", (F.col('Close') - F.col('Open')) /F.col('Open') *100)
    return daily_return_rate_df.select("Date", "daily_return_rate")
 
# Example usage:
# Specify the stock name, start date, and end date
stock_name = 'AMZN'  # Replace with the desired stock symbol (e.g., 'AAPL' for Apple, 'FB' for Facebook)
start_date = '2017-01-03'
end_date = '2017-08-05'
 
# Call the function
average_daily_return = calculate_average_daily_return(df, stock_name, start_date, end_date)
 
# Display the result
print(f"Average Daily Return for {stock_name} between {start_date} and {end_date}: {average_daily_return}")
 

# COMMAND ----------

def calculate_moving_average(stock_data, stock_name, start_date, end_date, window_size):
    # Filter stock data for the specified stock and date range
    filtered_data = stock_data.filter((F.col('stock_name') == stock_name) & (F.col('Date') >= start_date) & (F.col('Date') <= end_date))

    # Calculate moving average over the opening price column
    window_spec = Window().orderBy('Date').rowsBetween(-window_size, 0)
    filtered_data = filtered_data.withColumn('Moving Average', F.avg('Open').over(window_spec))

    return filtered_data.select('Date', 'Open', 'Moving Average')

# Example usage:
# Specify the stock name, start date, and end date
stock_name = 'AMZN'  # Replace with the desired stock symbol (e.g., 'AAPL' for Apple, 'FB' for Facebook)
start_date = '2017-01-03'
end_date = '2017-08-05'
window_size = 5

# Call the function
moving_average_data = calculate_moving_average(df, stock_name, start_date, end_date, window_size)

# Display the result
moving_average_data.show()

# COMMAND ----------

#%pip install --upgrade azure-storage-blob

# COMMAND ----------

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, PublicAccess

account_name = "pythonazurestorage"
account_key = "qwl1siWmjZ3+iHfonHHcOoMblTcIlpMEcgNQ5iaV/x1Y/oaeqzVscALyXJyj9f6pT2+N2iI+qkln+AStVLtYjQ=="
account_url = f"https://{account_name}.blob.core.windows.net"
container_name = "outputs"

# COMMAND ----------

blob_service = BlobServiceClient(credential=account_key , account_url = account_url)

container_client = blob_service.get_container_client(container_name)

# COMMAND ----------

result1_str = average_daily_return.toPandas().to_string()
 
result1_outputfile = 'result1.txt'

# COMMAND ----------

result2_str = moving_average_data.toPandas().to_string()

result2_outputfile = 'result2.txt'

# COMMAND ----------

blob_client = container_client.get_blob_client(result1_outputfile)
blob_client.upload_blob(result1_str, overwrite=True)

blob_client = container_client.get_blob_client(result2_outputfile)
blob_client.upload_blob(result2_str, overwrite=True)
