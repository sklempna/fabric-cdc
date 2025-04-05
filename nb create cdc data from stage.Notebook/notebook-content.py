# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "cba40a4b-9e86-4e62-acf9-75659ba30d31",
# META       "default_lakehouse_name": "lh_staging",
# META       "default_lakehouse_workspace_id": "7ec1d11b-c869-4880-a5c4-9acc27d23475",
# META       "known_lakehouses": [
# META         {
# META           "id": "cba40a4b-9e86-4e62-acf9-75659ba30d31"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

load_dts      = None
run_id = None
prev_run_id = None
first_import  = None
source_entity = None
pk_column = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import lit, to_timestamp, col, concat_ws, md5

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if first_import:
    df = spark.read.parquet(f"Files/staging_{source_entity}/{run_id}")
    df = df.withColumn("_cdc_operation", lit("insert"))
    df = df.withColumn("load_dts", to_timestamp(df["load_dts"]))
    df = df.withColumnRenamed("load_dts", "_cdc_load_dts")
    df.write.format("delta").mode("overwrite").saveAsTable(f"cdc_{source_entity}")
    notebookutils.notebook.exit(0)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

current_df = spark.read.parquet(f"Files/staging_{source_entity}/{run_id}")
previous_df = spark.read.parquet(f"Files/staging_{source_entity}/{prev_run_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

hashed_df = df.withColumn(
    "row_hash",  # New column for the hash
    md5(concat_ws("||", *[col(c) for c in df.columns if c != 'load_dts']))  # Concatenate all columns with a delimiter
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

current_df = current_df.withColumn(
    "row_hash",  # New column for the hash
    md5(concat_ws("||", *[col(c) for c in current_df.columns if c != 'load_dts']))  # Concatenate all columns with a delimiter
)

previous_df = previous_df.withColumn(
    "row_hash",  # New column for the hash
    md5(concat_ws("||", *[col(c) for c in previous_df.columns if c != 'load_dts']))  # Concatenate all columns with a delimiter
)

for c in previous_df.columns:
    previous_df = previous_df.withColumnRenamed(c, f"prev_{c}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

left_df = hashed_df.filter(col('load_dts') == load_dts)
right_df = hashed_df.filter(col('load_dts') == prev_load_dts).withColumnRenamed(pk_column, f"right_{pk_column}").withColumnRenamed("row_hash", "right_row_hash")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

update_df = join_df.filter(col(f"prev_{pk_column}").isNotNull() & (col("row_hash") != col("prev_row_hash")))
update_df = update_df.select(current_df.columns)
update_df = update_df.withColumn("load_dts", to_timestamp(col("load_dts"))).withColumnRenamed("load_dts", "_cdc_load_dts")
update_df = update_df.withColumn("_cdc_operation", lit("update"))
update_df = update_df.drop("row_hash")
display(update_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

delete_df = previous_df.join(current_df, previous_df[f"prev_{pk_column}"] == current_df[pk_column], "left").filter(col(pk_column).isNull())
delete_df = delete_df.select(previous_df.columns)
for c in delete_df.columns:
    delete_df = delete_df.withColumnRenamed(c, c[5:])

delete_df = delete_df.withColumn("load_dts", to_timestamp(col("load_dts"))).withColumnRenamed("load_dts", "_cdc_load_dts")
delete_df = delete_df.withColumn("_cdc_operation", lit("delete"))
delete_df = delete_df.drop("row_hash")
display(delete_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

insert_df.write.format("delta").mode("append").saveAsTable(f"cdc_{source_entity}")
update_df.write.format("delta").mode("append").saveAsTable(f"cdc_{source_entity}")
delete_df.write.format("delta").mode("append").saveAsTable(f"cdc_{source_entity}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
