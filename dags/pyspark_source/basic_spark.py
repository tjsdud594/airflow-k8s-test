import pyspark
from delta import configure_spark_with_delta_pip

#  Create a spark session with Delta
builder = pyspark.sql.SparkSession.builder.appName("DeltaTutorial") \
    .config("spark.jars.packages", "delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "https://play.min.io") \
    .config("fs.s3a.access.key", "Q3AM3UQ867SPQQA43P2F") \
    .config("fs.s3a.secret.key", "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG") \
    .config("fs.s3a.path.style.access", "true") \
    .config("fs.s3a.connection.ssl.enabled", "true") \
    .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .enableHiveSupport()

print("Spark Builder 완료")

# # Create spark context

spark = configure_spark_with_delta_pip(builder).getOrCreate()
# Log Level
spark.sparkContext.setLogLevel("WARN") # ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN

# Talbe path and name
# data_path = "/Users/syryu/Desktop/work/bigdata_platform/pyspark/delta_table"
data_path = "s3a://syryu-spark-test/delta_table"

# Create Table
data = spark.range(0, 5)
data.write.format("delta").save(data_path)

# Create Table(SQL)
# create_sql = """CREATE TABLE IF NOT EXISTS delta.`/Users/syryu/Desktop/work/bigdata_platform/pyspark/delta_table` ( \
# 	`key` STRING, \
# 	`value` STRING, \
# 	`topic` STRING, \
# 	`timestamp` TIMESTAMP, \
#     `date` STRING \
# ) \
# USING DELTA \
# PARTITIONED BY (date) \
# LOCATION '/Users/syryu/Desktop/work/bigdata_platform/pyspark/delta_table' \
# TBLPROPERTIES ( \
#     'delta.compatibility.symlinkFormatManifest.enabled'='true' \
# )"""
#
# spark_created_sql = spark.sql(create_sql)
#
df = spark.read.format("delta").load(data_path)
df.show()