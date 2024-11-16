from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Financial Data to HBase") \
    .getOrCreate()

df = spark.read.csv("hdfs://localhost:9000/usr/bitnami/BTCUSD_1D_OKX.csv", header=True, inferSchema=True)

df.show()

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

def create_hbase_row(row):

    row_key = row['timestamp']

    column_values = {
        'prices:Open': row['Open'],
        'prices:High': row['High'],
        'prices:Low': row['Low'],
        'prices:Close': row['Close'],
        'volume:Volume': row['Volume'],
        'volume:Volume (Currency)': row['Volume (Currency)'],
        'transaction:weekday_name': row['weekday_name'],
        'transaction:Average_Transaction_Price': row['Average_Transaction_Price']
    }
    return (row_key, column_values)

rdd = df.rdd.map(create_hbase_row)

from pyspark.hbase import HBaseContext
hbase_context = HBaseContext(spark.sparkContext)

hbase_context.bulkPut(rdd, "financial_data", "timestamp")
