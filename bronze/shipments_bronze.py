%md
# Bronze to Silver: Data Cleansing and Transformation
%md
## order returns
from pyspark.sql.types import StringType, IntegerType, DateType, BooleanType
import pyspark.sql.functions as F
from delta.tables import DeltaTable
%md
#### Create Widgets
dbutils.widgets.text("catalog_name", "ecommerce", "Catalog Name")
dbutils.widgets.text("storage_account_name", "ecommcistorage05", "Storage Account Name")
dbutils.widgets.text("container_name", "ecomm-raw-data", "Container Name")
catalog_name = dbutils.widgets.get("catalog_name")
storage_account_name = dbutils.widgets.get("storage_account_name")
container_name = dbutils.widgets.get("container_name")
%md
### Stream Bronze Table in a Dataframe

df = spark.readStream \
     .format("Delta") \
     .table(f"{catalog_name}.bronze.brz_order_returns")
df = df.dropDuplicates(["order_dt", "order_id","return_ts"])


df = df.withColumn("return_ts", F.to_timestamp(F.col("return_ts"), "yyyy-MM-dd'T'HH:mm:ss"))\
       .withColumn("order_dt", F.to_date(F.col("order_dt"))) \
       .withColumn("reason", F.trim(F.upper(F.col("reason"))))\
       .withColumnRenamed("ingest_timestamp", "processed_time")
silver_checkpoint_path1 = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoint/silver/fact_order_return/"
print(silver_checkpoint_path1)
def upsert_to_silver(microBatchDF, batchId):
    table_name = f"{catalog_name}.silver.slv_order_returns"
    if not spark.catalog.tableExists(table_name):
        print("creating new table")
        microBatchDF.write.format("delta").mode("overwrite").saveAsTable(table_name)
        spark.sql(
            f"ALTER TABLE {table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
        )
    else:
        deltaTable = DeltaTable.forName(spark, table_name)
        deltaTable.alias("silver_table").merge(
            microBatchDF.alias("batch_table"),
            "silver_table.order_id = batch_table.order_id" ,
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()    

    

# This line is running a Structured Streaming job that:
# - Reads incremental data from Bronze (df).
# - For each batch → applies upsert_to_silver (update if exists, insert if not).
# - Writes into a Silver Delta table with schema evolution enabled.
# - Uses checkpointing for recovery.
# - Runs in batch-like mode (once or availableNow), not continuous streaming.

df.writeStream.trigger(availableNow=True).foreachBatch(
    upsert_to_silver
).format("delta").option("checkpointLocation", silver_checkpoint_path1).option(
    "mergeSchema", "true"
).outputMode(
    "update"
).trigger(
    once=True
).start().awaitTermination()
%md
## order shipments
df1 = spark.readStream \
     .format("Delta") \
     .table(f"{catalog_name}.bronze.brz_order_shipments")
df1 = df1.withColumn("order_dt", F.to_date(F.col("order_dt"))) \
       .withColumn("carrier", F.upper(F.col("carrier")))\
       .withColumnRenamed("ingest_timestamp", "processed_time")
silver_checkpoint_path2 = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoint/silver/fact_order_shipment/"
print(silver_checkpoint_path2)
def upsert_to_silver(microBatchDF, batchId):
    table_name = f"{catalog_name}.silver.slv_order_shipments"
    if not spark.catalog.tableExists(table_name):
        print("creating new table")
        microBatchDF.write.format("delta").mode("overwrite").saveAsTable(table_name)
        spark.sql(
            f"ALTER TABLE {table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
        )
    else:
        deltaTable = DeltaTable.forName(spark, table_name)
        deltaTable.alias("silver_table").merge(
            microBatchDF.alias("batch_table"),
            "silver_table.order_id = batch_table.order_id AND silver_table.shipment_id = batch_table.shipment_id",
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()    

    

# This line is running a Structured Streaming job that:
# - Reads incremental data from Bronze (df).
# - For each batch → applies upsert_to_silver (update if exists, insert if not).
# - Writes into a Silver Delta table with schema evolution enabled.
# - Uses checkpointing for recovery.
# - Runs in batch-like mode (once or availableNow), not continuous streaming.

df1.writeStream.trigger(availableNow=True).foreachBatch(
    upsert_to_silver
).format("delta").option("checkpointLocation", silver_checkpoint_path2).option(
    "mergeSchema", "true"
).outputMode(
    "update"
).trigger(
    once=True
).start().awaitTermination()
