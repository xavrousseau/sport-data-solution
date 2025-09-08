from pyspark.sql import SparkSession
spark = SparkSession.builder.appName(jmx-smoketest).getOrCreate()
spark.range(500_000).selectExpr(sum(id)).collect()
import time; time.sleep(60)  # garde le driver vivant 60s
spark.stop()
