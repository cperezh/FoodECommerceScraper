from msilib import schema
import math
from SparkDBUtils import SparkDB
from pyspark.sql.types import DateType, StructType, StructField, IntegerType, TimestampType
import pyspark.sql.functions as psf
from pyspark.sql.window import Window
import datetime as dt

schema = StructType([\
        StructField("id", IntegerType(), True),\
        StructField("date", DateType(), True),\
        StructField("ts_load", TimestampType(), True),\
        ])


def create_db(spark):

    sequences_cfg = """
            CREATE OR REPLACE TABLE sequences_cfg
            (
                table_name STRING,
                id BIGINT,
                ts_load timestamp
            ) USING DELTA
        """

    spark.sql(sequences_cfg)

    date_dim = """
        CREATE OR REPLACE TABLE date_dim
        (
            id_date int,
            date date,
            ts_load timestamp
        ) USING DELTA;
        """

    spark.sql(date_dim)

    conf = f"""
        INSERT INTO sequences_cfg VALUES('date_dim',0,'{dt.datetime.now()}')
    """

    spark.sql(conf)


if __name__ == "__main__":

    sparkdb = SparkDB()

    create_db(sparkdb.spark)
