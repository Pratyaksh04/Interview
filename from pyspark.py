from pyspark.sql import SparkSession # type: ignore
from pyspark.sql import functions as f # type: ignore

spark = SparkSession.builder \
    .appName("ReadParquet") \
    .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
    .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
    .config("spark.sql.legacy.parquet.nanosAsLong", "true") \
    .getOrCreate()


spark.sparkContext.setLogLevel("ERROR")



base_path = ("D:\OneDrive\Desktop\BD Group Interview\MS_Contoso_Sample_Data_Set_Parquet")
sales = spark.read.parquet(f"{base_path}\Sales.parquet")

calendar = spark.read.parquet(f"{base_path}\Calendar.parquet")

channel = spark.read.parquet(f"{base_path}\Channel.parquet").alias("ch")

stores = spark.read.parquet(f"{base_path}\Stores.parquet")

geo = spark.read.parquet(f"{base_path}\Geography.parquet")

product = spark.read.parquet(f"{base_path}\Product.parquet")

prod_sub = spark.read.parquet(f"{base_path}\ProductSubcategory.parquet")

prod_cat = spark.read.parquet(f"{base_path}\ProductCategory.parquet")

promo = spark.read.parquet(f"{base_path}\Promotion.parquet")



result = sales.join(channel, sales.channelKey == channel.Channel) \
         .groupBy("ChannelName")\
         .agg(f.round(f.sum("SalesAmount"),2).alias("total_sales"))\
         .orderBy(f.desc("total_sales"))

open_store = stores.filter((f.col("status")=="On") & (f.col("EmployeeCount")>10))

# sales.show(10)


# clean_sales = sales.fillna({"DiscountAmount":1000, "ReturnAmount":0})

# clean_sales.filter(f.col("DiscountAmount").isNull()).show(10)


sales.select("channelKey").distinct().show()


join_table = sales.join(stores, sales.StoreKey == stores.StoreKey, "inner")
channel.printSchema()
join_table.select(f.col("ch.channel"), f.col("SalesKey")).show(5)