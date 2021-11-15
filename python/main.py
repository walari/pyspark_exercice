import yaml

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, TimestampType, StringType, IntegerType, DoubleType

from utils import csv_to_db, CUSTOMER_STRUCTURE, ITEM_STRUCTURE, ORDER_STRUCTURE, PRODUCT_STRUCTURE

if __name__ == "__main__":
    # conf = SparkConf().setAppName('free2move').setMaster('local[2]')
    # sc = SparkContext(conf = conf)
    # customers = sc.textFile('data/customer.csv')

    config_pathfile = 'config.yaml'
    
    config = None
    with open(config_pathfile, "r") as f:
        config = yaml.safe_load(f)

    spark = SparkSession \
        .builder \
        .appName("Pyspark test application") \
        .config("spark.jars", config['db'].get('driver_filepath')) \
        .getOrCreate()

    # Extract csv data and store in Postgres DB
    # csv_to_db(spark, config.get('db'),
    #     data_filepath = config.get('customer_filepath'),
    #     data_structure=CUSTOMER_STRUCTURE,
    #     table_name='customer')
    # csv_to_db(spark, config.get('db'),
    #     data_filepath = config.get('item_filepath'),
    #     data_structure=ITEM_STRUCTURE,
    #     table_name='item')
    # csv_to_db(spark, config.get('db'),
    #     data_filepath = config.get('order_filepath'),
    #     data_structure=ORDER_STRUCTURE,
    #     table_name='order')
    # csv_to_db(spark, config.get('db'),
    #     data_filepath = config.get('product_filepath'),
    #     data_structure=PRODUCT_STRUCTURE,
    #     table_name='product')

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

    # order_df.printSchema()

    order_df \
        .join(item_df,order_df.order_id ==  item_df.order_id,"left") \
        .join(product_df,item_df.product_id ==  product_df.product_id,"left") \
        .show(10, False)

    # print(f"nb_lines: {customers.count()}")