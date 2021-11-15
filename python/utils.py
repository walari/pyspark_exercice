import re

from pyspark.sql.types import StructType, TimestampType, StringType, IntegerType, DoubleType

CUSTOMER_STRUCTURE = StructType() \
      .add("customer_id",StringType(),True) \
      .add("customer_unique_id",StringType(),True) \
      .add("customer_zip_code_prefix",StringType(),True) \
      .add("customer_city",StringType(),True) \
      .add("customer_state",StringType(),True)
ITEM_STRUCTURE = StructType() \
    .add("order_id",StringType(),True) \
    .add("order_item_id",IntegerType(),True) \
    .add("product_id",StringType(),True) \
    .add("seller_id",StringType(),True) \
    .add("shipping_limit_date",TimestampType(),True) \
    .add("price",DoubleType(),True) \
    .add("freight_value",DoubleType(),True)
ORDER_STRUCTURE = StructType() \
    .add("order_id",StringType(),True) \
    .add("customer_id",StringType(),True) \
    .add("order_status",StringType(),True) \
    .add("order_purchase_timestamp",TimestampType(),True) \
    .add("order_approved_at",TimestampType(),True) \
    .add("order_delivered_carrier_date",TimestampType(),True) \
    .add("order_delivered_customer_date",TimestampType(),True) \
    .add("order_estimated_delivery_date",TimestampType(),True)
PRODUCT_STRUCTURE = StructType() \
    .add("product_id",StringType(),True) \
    .add("product_category_name",StringType(),True) \
    .add("product_name_lenght",DoubleType(),True) \
    .add("product_description_lenght",DoubleType(),True) \
    .add("product_photos_qty",DoubleType(),True) \
    .add("product_weight_g",DoubleType(),True) \
    .add("product_length_cm",DoubleType(),True) \
    .add("product_height_cm",DoubleType(),True) \
    .add("product_width_cm",DoubleType(),True) \
    .add("product_category_name_english",StringType(),True)

def csv_to_db(spark, db_config, data_filepath, data_structure, table_name):
    df = spark.read.format("csv") \
        .option("header", True) \
        .option("dateFormat", "yyyy-MM-dd HH:mm:ss") \
        .schema(data_structure) \
        .load(data_filepath)
    df.printSchema()
    df.write.format("jdbc") \
            .mode("append") \
            .option("url", db_config.get('url')) \
            .option("dbtable", f"{db_config.get('schema_name')}.{table_name}") \
            .option("user", db_config.get('user')) \
            .option("password", db_config.get('password')) \
            .option("driver", db_config.get('driver')) \
            .save()

COMMA_DELIM = re.compile(''',(?=(?:[^"]*"[^"]*")*[^"]*$)''')