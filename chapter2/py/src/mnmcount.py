from __future__ import print_function
from pyspark.sql import SparkSession
import os

os.environ["JAVA_HOME"] = "/home/trofficus/jre1.8.0_301"

DATA_PATH = "./data/mnm_dataset.csv"

spark = (SparkSession
         .builder
         .appName("PythonMnMCount")
         .getOrCreate())

mnm_df = (spark.read.format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(DATA_PATH))

mnm_df.show(n=5, truncate=False)

# aggregate count of all colors and groupBy state and color
# orderBy descending order

count_mnm_df = (mnm_df.select("State", "Color", "Count")
                .groupBy("State", "Color")
                .sum("Count")
                .orderBy("sum(Count)", ascending=False))

# show all the resulting aggregation for all the dates and colors
count_mnm_df.show(n=60, truncate=False)
print("Total Rows = %d" % (count_mnm_df.count()))

# find the aggregate count for California by filtering
ca_count_mnm_df = (mnm_df.select("*")
                   .where(mnm_df.State == 'CA')
                   .groupBy("State", "Color")
                   .sum("Count")
                   .orderBy("sum(Count)", ascending=False))

# find the sum of california
ca_total_mnm_df = (mnm_df.select("State", "Count")
                   .where(mnm_df.State == "CA"))\
    .groupBy("State")\
    .sum("Count")\
    .orderBy("sum(Count)", ascending=True)

# show the resulting aggregation for California
ca_count_mnm_df.show(n=10, truncate=False)
ca_total_mnm_df.show(n=1, truncate=False)

spark.stop()
