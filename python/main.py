import yaml

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date

from utils import csv_to_db, CUSTOMER_STRUCTURE, ITEM_STRUCTURE, ORDER_STRUCTURE, PRODUCT_STRUCTURE

CONFIG_FILEPATH = 'config.yaml'

if __name__ == "__main__":
    
    config = None
    with open(CONFIG_FILEPATH, "r") as f:
        config = yaml.safe_load(f)

    spark = SparkSession \
        .builder \
        .appName("Pyspark test application") \
        .config("spark.jars", config['db'].get('driver_filepath')) \
        .getOrCreate()

    # Extract csv data and store in Postgres DB
    csv_to_db(spark, config.get('db'),
        data_filepath = config.get('customer_filepath'),
        data_structure=CUSTOMER_STRUCTURE,
        table_name='customer')
    csv_to_db(spark, config.get('db'),
        data_filepath = config.get('item_filepath'),
        data_structure=ITEM_STRUCTURE,
        table_name='item')
    csv_to_db(spark, config.get('db'),
        data_filepath = config.get('order_filepath'),
        data_structure=ORDER_STRUCTURE,
        table_name='order')
    csv_to_db(spark, config.get('db'),
        data_filepath = config.get('product_filepath'),
        data_structure=PRODUCT_STRUCTURE,
        table_name='product')

    # Compute customer stats
    def db_extract(spark, db_config, table_name):
        return spark.read \
            .format("jdbc") \
            .option("url", db_config.get('url')) \
            .option("driver", db_config.get('driver')) \
            .option("dbtable", f"{db_config.get('schema_name')}.{table_name}") \
            .option("user", db_config.get('user')) \
            .option("password", db_config.get('password')) \
            .load()

    customer_df = db_extract(spark, config['db'], 'customer')
    order_df = db_extract(spark, config['db'], 'order')
    item_df = db_extract(spark, config['db'], 'item')
    product_df = db_extract(spark, config['db'], 'product')

    # Compute daily orders
    if False:
        def get_daily_orders(daily_orders_df, order_date):
            return daily_orders_df \
                .filter(daily_orders_df.order_date == order_date)

        daily_orders_df = customer_df \
            .join(order_df, customer_df.customer_id == order_df.customer_id, "left") \
            .withColumn("order_date", to_date("order_purchase_timestamp")) \
            .groupBy("customer_unique_id","order_date") \
            .count()

        get_daily_orders(daily_orders_df, '2018-05-28') \
            .sort(desc("count")) \
            .show(10, False)