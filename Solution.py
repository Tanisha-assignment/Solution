
from pyspark.sql import SparkSession 
from pyspark.sql.window import Window
from pyspark.sql.functions import col, min, max, sum

v_path = f'test/sample_test'   #specify folder path to your storage system
spark = SparkSession.builder.appName("SignalAnalysis").getOrCreate() 
signals_df = spark.read.option("mergeSchema", "true").parquet(v_path)

signals_df = signals_df.withColumn("month_id", signals_df["month_id"].cast("int"))
window_spec = Window.partitionBy("entity_id")
signals_df = signals_df \
    .withColumn("oldest_month_id", min("month_id").over(window_spec)) \
    .withColumn("newest_month_id", max("month_id").over(window_spec))\
    .withColumn("item_id_oldest", when((col("month_id") == col("oldest_month_id")),col("item_id")))\
    .withColumn("item_id_newest",when(col("month_id") == col("newest_month_id"), col("item_id")))\
    .withColumn("total_signals", sum("signal_count").over(window_spec))

signals_df = signals_df.withColumn("oldest_item_id", min(when(col("item_id_oldest").isNotNull(), col("item_id_oldest")).otherwise(col("item_id_oldest"))).over(window_spec))
signals_df = signals_df.withColumn("newest_item_id", min(when(col("item_id_newest").isNotNull(), col("item_id_newest")).otherwise(col("item_id_newest"))).over(window_spec))

signals_df = signals_df.select("entity_id","oldest_item_id","newest_item_id","total_signals")
signals_df = signals_df.dropDuplicates(['entity_id'])
signals_df.write.parquet("test/final_data.parquet")  #write file to your storage system folder
