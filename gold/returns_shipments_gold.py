%md
# order returns
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
# readChangeFeed flag is used to read the change feed (_change_type column mainly)
df = spark.readStream \
.format("delta") \
.table(f"{catalog_name}.silver.slv_order_returns")

df = df.withColumn("date_id", F.date_format(F.col("return_ts"), "yyyyMMdd").cast(IntegerType()))
df = df.withColumn("return_days", F.datediff(F.col("return_ts"), F.col("order_dt")))
df = df.withColumn("within_policy", F.when(F.col("return_days").between(0, 15), F.lit(1)).otherwise(F.lit(0)))
df = df.withColumn("is_late_return", F.when(F.col("return_days") > 15, F.lit(1)).otherwise(F.lit(0)))


df = df.select(
    F.col("date_id") ,
    F.col("order_dt") ,
    F.col("order_id") ,
    F.col("return_days") ,
    F.col("return_ts") ,
    F.col("reason") ,
    F.col("is_late_return") ,
    F.col("within_policy") ,
    F.col("processed_time") ,
    F.col("source_file")
   
)
gold_checkpoint_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoint/gold/order_return/"
print(gold_checkpoint_path)

def upsert_to_gold(microBatchDF, batchId):
    table_name = f"{catalog_name}.gold.gld_order_returns"
    if not spark.catalog.tableExists(table_name):
        print("creating new table")
        microBatchDF.write.format("delta").mode("overwrite").saveAsTable(table_name)
        spark.sql(
            f"ALTER TABLE {table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
        )
    else:
        deltaTable = DeltaTable.forName(spark, table_name)
        deltaTable.alias("gold_table").merge(
            microBatchDF.alias("batch_table"),
            "gold_table.order_id = batch_table.order_id",
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

df.writeStream.trigger(availableNow=True).foreachBatch(
    upsert_to_gold
).format("delta").option("checkpointLocation", gold_checkpoint_path).option(
    "mergeSchema", "true"
).outputMode(
    "update"
).trigger(
    once=True
).start().awaitTermination()
%md
#order shipments 
# readChangeFeed flag is used to read the change feed (_change_type column mainly)
df1 = spark.readStream \
.format("delta") \
.option("skipChangeCommits", "true") \
.table(f"{catalog_name}.silver.slv_order_shipments")
df1 = df1.withColumn(
    "carrier_group",
    F.when(
        F.col("carrier").isin("ECOMEXPRESS", "DELHIVERY", "XPRESSBEES", "BLUEDART"),
        F.lit("Domestic")
    ).otherwise(F.lit("International"))
)

df1 = df1.withColumn(
    "is_weekend_shipment",
    F.when(F.weekday("order_dt").isin(5, 6), F.lit(True)).otherwise(F.lit(False))
)
df1 = df1.select(
    F.col("order_id"),
    F.col("shipment_id"),
    F.col("order_dt"),
    F.col("carrier"),
    F.col("carrier_group"),
    F.col("is_weekend_shipment"),
    F.col("processed_time")
)
gold_checkpoint_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoint/gold/order_shipment/"
print(gold_checkpoint_path)

def upsert_to_gold(microBatchDF, batchId):
    table_name = f"{catalog_name}.gold.gld_order_shipments"
    if not spark.catalog.tableExists(table_name):
        print("creating new table")
        microBatchDF.write.format("delta").mode("overwrite").saveAsTable(table_name)
        spark.sql(
            f"ALTER TABLE {table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
        )
    else:
        deltaTable = DeltaTable.forName(spark, table_name)
        deltaTable.alias("gold_table").merge(
            microBatchDF.alias("batch_table"),
            "gold_table.order_id = batch_table.order_id AND gold_table.shipment_id = batch_table.shipment_id",
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

df1.writeStream.trigger(availableNow=True).foreachBatch(
    upsert_to_gold
).format("delta").option("checkpointLocation", gold_checkpoint_path).option(
    "mergeSchema", "true"
).outputMode(
    "update"
).start().awaitTermination()
