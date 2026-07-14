# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a6310158-d270-4ca9-9de4-ec6285ceccb4",
# META       "default_lakehouse_name": "bronze_lakehouse",
# META       "default_lakehouse_workspace_id": "3deade31-2f59-49b7-88b1-9b104ce28d0a",
# META       "known_lakehouses": [
# META         {
# META           "id": "a6310158-d270-4ca9-9de4-ec6285ceccb4"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Add profit_amount column using PySpark DataFrame API

from pyspark.sql.functions import col

# Read existing table
df_sales = spark.table("dbo.fct_sales")

# Add profit_amount as extended_price - cost_amount, cast to DECIMAL(18,2)
df_sales_with_profit = df_sales.withColumn(
    "profit_amount",
    (col("extended_price") - col("cost_amount"))
)

# Overwrite the existing table with the new column included
# NOTE: This rewrites the whole table; ensure that's acceptable before running.
df_sales_with_profit.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("dbo.fct_sales")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
