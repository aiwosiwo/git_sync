from pyspark.sql import SparkSession

# 创建 Spark 会话
spark = SparkSession.builder \
    .appName("Financial Data to HBase") \
    .getOrCreate()

# 读取 CSV 文件
df = spark.read.csv("hdfs://localhost:9000/usr/bitnami/BTCUSD_1D_OKX.csv", header=True, inferSchema=True)

df.show()

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# 准备将数据转换成 HBase 所需的格式
def create_hbase_row(row):
    # 假设 'timestamp' 列是 RowKey
    row_key = row['timestamp']
    # 为 HBase 写入的数据构建列族和列值
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

# 使用 RDD 转换数据
rdd = df.rdd.map(create_hbase_row)

# 写入 HBase
from pyspark.hbase import HBaseContext
hbase_context = HBaseContext(spark.sparkContext)

# 使用 HBaseContext 将数据写入 HBase
hbase_context.bulkPut(rdd, "financial_data", "timestamp")
