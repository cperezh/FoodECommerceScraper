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
            id int,
            date date,
            ts_load timestamp
        ) USING DELTA;
        """

    spark.sql(date_dim)

    conf = f"""
        INSERT INTO sequences_cfg VALUES('date_dim',0,'{dt.datetime.now()}')
    """

    spark.sql(conf)


def prueba(sparkdb: SparkDB):

    df_new = sparkdb.spark.createDataFrame([
        (None, dt.datetime(2020, 5, 17), dt.datetime.now()),
        (None, dt.datetime(2020, 5, 25), dt.datetime.now())],
        schema=schema)

    df_new = sparkdb.insert_id(df_new)

    df_new.show()

    sparkdb.write_table(df_new, "date_dim", "append")

    sparkdb.read_table("date_dim").show()


if __name__ == "__main__":

    sparkdb = SparkDB()

    # create_db(sparkdb.spark)

    prueba(sparkdb)
