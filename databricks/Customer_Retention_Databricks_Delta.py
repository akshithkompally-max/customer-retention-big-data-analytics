# Databricks notebook source
# MAGIC %md
# MAGIC # Identifying & Retaining High-Value Customers - Databricks + Delta Lake
# MAGIC **Dataset:** UCI Online Retail (541,909 invoice lines, Dec-2010 to Dec-2011)  
# MAGIC **Architecture:** Bronze (raw) -> Silver (clean sales) -> Gold (customer RFM + segments), all stored as **Delta tables**.  
# MAGIC **How to run:** Import this file into your workspace (Workspace -> Import), attach to compute, upload the CSV (see Cell 2), then *Run all*.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration
# MAGIC Upload `online_retail_raw.csv` to a Unity Catalog Volume:  
# MAGIC *Catalog -> workspace -> default -> Create -> Volume (name: `retail_data`) -> Upload to this volume.*  
# MAGIC If your workspace has no Volumes, use *+ New -> Add or upload data -> Upload files* and copy the path it shows.

# COMMAND ----------

DATA_PATH = "/Volumes/workspace/default/retail_data/online_retail_raw.csv"   # <- change if your path differs
SCHEMA = "default"          # tables are created as <schema>.<name>

if "display" not in globals():          # lets the same code run outside Databricks
    display = lambda df, *a, **k: df.show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load raw data with an explicit schema (Bronze)

# COMMAND ----------

from pyspark.sql import functions as F, Window
from pyspark.sql.types import *

raw_schema = StructType([
    StructField("InvoiceNo", StringType()), StructField("StockCode", StringType()),
    StructField("Description", StringType()), StructField("Quantity", IntegerType()),
    StructField("InvoiceDate", TimestampType()), StructField("UnitPrice", DoubleType()),
    StructField("CustomerID", DoubleType()), StructField("Country", StringType())])

bronze = (spark.read.option("header", True).schema(raw_schema).csv(DATA_PATH))
print("Rows loaded:", bronze.count())
bronze.printSchema()
display(bronze.limit(5))

# COMMAND ----------

# Data-quality profile: nulls per column
nulls = bronze.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in bronze.columns])
display(nulls)

# COMMAND ----------

# Save as a Delta table (Bronze = raw copy, nothing removed)
(bronze.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(f"{SCHEMA}.retail_bronze"))
print("Bronze Delta table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Clean and transform (Silver)
# MAGIC Rules (same as the Python notebook and the PostgreSQL scripts): drop exact duplicates; remove cancellations (`InvoiceNo` starts with C); keep Quantity > 0 and UnitPrice > 0; remove non-product codes; keep only rows with a CustomerID.

# COMMAND ----------

NON_PRODUCT = ["POST", "D", "M", "DOT", "CRUK", "BANK CHARGES", "S", "AMAZONFEE", "B", "PADS", "C2"]

silver = (spark.table(f"{SCHEMA}.retail_bronze")
    .dropDuplicates()
    .filter(~F.col("InvoiceNo").startswith("C"))
    .filter((F.col("Quantity") > 0) & (F.col("UnitPrice") > 0))
    .filter(~F.col("StockCode").isin(NON_PRODUCT))
    .filter(F.col("CustomerID").isNotNull())
    .withColumn("CustomerID", F.col("CustomerID").cast("int"))
    .withColumn("Revenue", F.round(F.col("Quantity") * F.col("UnitPrice"), 2))
    .withColumn("InvoiceMonth", F.date_trunc("month", "InvoiceDate").cast("date")))

(silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(f"{SCHEMA}.retail_silver"))
s = spark.table(f"{SCHEMA}.retail_silver")
print("Silver rows:", s.count(), "| customers:", s.select("CustomerID").distinct().count(),
      "| invoices:", s.select("InvoiceNo").distinct().count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Filters and aggregations (business questions)

# COMMAND ----------

# Headline KPIs
kpi = s.agg(F.round(F.sum("Revenue"), 2).alias("total_revenue"),
            F.countDistinct("InvoiceNo").alias("orders"),
            F.countDistinct("CustomerID").alias("customers"))
display(kpi.withColumn("avg_order_value", F.round(F.col("total_revenue") / F.col("orders"), 2)))

# COMMAND ----------

# Monthly revenue trend
monthly = (s.groupBy("InvoiceMonth").agg(F.round(F.sum("Revenue"), 0).alias("revenue"),
                                         F.countDistinct("InvoiceNo").alias("orders"),
                                         F.countDistinct("CustomerID").alias("active_customers"))
            .orderBy("InvoiceMonth"))
display(monthly)

# COMMAND ----------

# Filter + aggregate: revenue by country excluding the UK (international opportunity)
intl = (s.filter(F.col("Country") != "United Kingdom").groupBy("Country")
          .agg(F.round(F.sum("Revenue"), 0).alias("revenue"), F.countDistinct("CustomerID").alias("customers"))
          .orderBy(F.desc("revenue")).limit(10))
display(intl)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Customer RFM and segments (Gold)

# COMMAND ----------

snapshot = F.lit("2011-12-10").cast("date")
cust = (s.groupBy("CustomerID")
          .agg(F.datediff(snapshot, F.max("InvoiceDate").cast("date")).alias("Recency"),
               F.countDistinct("InvoiceNo").alias("Frequency"),
               F.round(F.sum("Revenue"), 2).alias("Monetary"),
               F.first("Country").alias("Country")))

w_r = Window.orderBy(F.col("Recency").desc())
w_f = Window.orderBy(F.col("Frequency").asc())
w_m = Window.orderBy(F.col("Monetary").asc())
scored = (cust.withColumn("R", F.ntile(5).over(w_r)).withColumn("F", F.ntile(5).over(w_f))
              .withColumn("M", F.ntile(5).over(w_m))
              .withColumn("FM", (F.col("F") + F.col("M")) / 2))

gold = scored.withColumn("Segment",
    F.when((F.col("R") >= 4) & (F.col("FM") >= 4.5), "Champions")
     .when((F.col("R") >= 3) & (F.col("FM") >= 3.5), "Loyal Customers")
     .when((F.col("R") >= 4) & (F.col("FM") >= 2.5), "Potential Loyalists")
     .when(F.col("R") >= 4, "New / Promising")
     .when(F.col("R") >= 3, "Need Attention")
     .when((F.col("R") <= 2) & (F.col("FM") >= 4), "Cant Lose Them")
     .when((F.col("R") <= 2) & (F.col("FM") >= 2.5), "At Risk")
     .otherwise("Hibernating / Lost"))

(gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
     .saveAsTable(f"{SCHEMA}.retail_gold_customer_rfm"))

g = spark.table(f"{SCHEMA}.retail_gold_customer_rfm")
seg = (g.groupBy("Segment").agg(F.count("*").alias("customers"), F.round(F.sum("Monetary"), 0).alias("revenue"),
                                F.round(F.avg("Recency"), 0).alias("avg_recency"),
                                F.round(F.avg("Frequency"), 1).alias("avg_orders"))
         .withColumn("pct_revenue", F.round(100 * F.col("revenue") / F.sum("revenue").over(Window.partitionBy()), 1))
         .orderBy(F.desc("revenue")))
display(seg)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Delta Lake features (evidence for the project)
# MAGIC ACID transactions, versioning (history + time travel), schema enforcement, MERGE (upsert).

# COMMAND ----------

# 6.1 Table details and transaction log
display(spark.sql(f"DESCRIBE DETAIL {SCHEMA}.retail_gold_customer_rfm"))

# COMMAND ----------

# 6.2 UPDATE creates a new table version (ACID)
spark.sql(f"ALTER TABLE {SCHEMA}.retail_gold_customer_rfm ADD COLUMNS (RetentionAction STRING)")
spark.sql(f"""UPDATE {SCHEMA}.retail_gold_customer_rfm SET RetentionAction = CASE
    WHEN Segment = 'Champions' THEN 'VIP programme, early access, referral rewards'
    WHEN Segment = 'Loyal Customers' THEN 'Upsell bundles, loyalty points'
    WHEN Segment = 'Cant Lose Them' THEN 'Personal outreach + win-back offer'
    WHEN Segment = 'At Risk' THEN 'Win-back campaign + survey'
    WHEN Segment = 'Potential Loyalists' THEN 'Second-purchase offer'
    WHEN Segment = 'New / Promising' THEN 'Welcome / onboarding journey'
    WHEN Segment = 'Need Attention' THEN 'Personalised reminder'
    ELSE 'Low-cost reactivation email' END""")

# COMMAND ----------

# 6.3 Version history (every write is logged)
display(spark.sql(f"DESCRIBE HISTORY {SCHEMA}.retail_gold_customer_rfm"))

# COMMAND ----------

# 6.4 Time travel - read the table as it was before the UPDATE (version 0)
old = spark.sql(f"SELECT * FROM {SCHEMA}.retail_gold_customer_rfm VERSION AS OF 0")
print("Columns in version 0:", old.columns)
print("Columns now        :", spark.table(f"{SCHEMA}.retail_gold_customer_rfm").columns)

# COMMAND ----------

# 6.5 MERGE (upsert): apply a correction for two customers (e.g. manager marks them VIP)
from delta.tables import DeltaTable
updates = spark.createDataFrame([(14646, "VIP - account manager assigned"),
                                 (18102, "VIP - account manager assigned")], ["CustomerID", "RetentionAction"])
tbl = DeltaTable.forName(spark, f"{SCHEMA}.retail_gold_customer_rfm")
(tbl.alias("t").merge(updates.alias("u"), "t.CustomerID = u.CustomerID")
    .whenMatchedUpdate(set={"RetentionAction": "u.RetentionAction"}).execute())
display(spark.sql(f"SELECT CustomerID, Monetary, Segment, RetentionAction FROM {SCHEMA}.retail_gold_customer_rfm "
                  f"WHERE CustomerID IN (14646, 18102)"))

# COMMAND ----------

# 6.6 Schema enforcement - Delta rejects data that does not match the table schema
bad = spark.createDataFrame([("abc", "xyz")], ["CustomerID", "Unexpected"])
try:
    bad.write.format("delta").mode("append").saveAsTable(f"{SCHEMA}.retail_gold_customer_rfm")
except Exception as e:
    print("Schema enforcement blocked the write:", type(e).__name__)

# COMMAND ----------

# 6.7 Optional: file compaction (may be unsupported on some compute types)
try:
    display(spark.sql(f"OPTIMIZE {SCHEMA}.retail_gold_customer_rfm"))
except Exception as e:
    print("OPTIMIZE not available on this compute:", type(e).__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validation against PostgreSQL and Python
# MAGIC Expected: 391,150 sales lines | 18,402 invoices | 4,334 customers | revenue about GBP 8,737,228.

# COMMAND ----------

chk = s.agg(F.count("*").alias("lines"), F.countDistinct("InvoiceNo").alias("invoices"),
            F.countDistinct("CustomerID").alias("customers"), F.round(F.sum("Revenue"), 2).alias("revenue"))
display(chk)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Export the Gold table for Power BI (optional)
# MAGIC *Catalog -> default -> retail_gold_customer_rfm -> Download as CSV* (or `display(g)` -> download).  
# MAGIC Power BI can also connect directly through the Databricks connector if your plan allows.
