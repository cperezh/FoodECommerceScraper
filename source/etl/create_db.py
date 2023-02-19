from msilib import schema
import math
from SparkDBUtils import SparkDB
from pyspark.sql.types import DateType, StructType, StructField, IntegerType
import pyspark.sql.functions as psf
from pyspark.sql.window import Window
import datetime as dt

schema = StructType([\
        StructField("id", IntegerType(), True),\
        StructField("date", DateType(), True),\
        StructField("timestamp", DateType(), True),\
        ])


def create_db(spark):

    sequences_cfg = """
            CREATE TABLE IF NOT EXISTS sequences_cfg
            (
                table_name STRING,
                id BIGINT,
                ts_load timestamp
            ) USING DELTA
        """

    spark.sql(sequences_cfg)

    date_dim = """
        CREATE TABLE IF NOT EXISTS date_dim
        (
            id BIGINT,
            date date,
            ts_load timestamp
        ) USING DELTA;
        """

    spark.sql(date_dim)

    conf = f"""
        INSERT INTO sequences_cfg VALUES('date_dim',0,'{dt.datetime.now()}')
    """

    spark.sql(conf)


def prueba():
    df = sparkdb.read_table("date_dim")

    df_new = sparkdb.spark.createDataFrame([
        (None, dt.datetime(2020, 5, 17), dt.datetime.now()),
        (None, dt.datetime(2020, 5, 25), dt.datetime.now())],
        schema=schema)

    maxim = df.pandas_api()["id_date"].max()

    if math.isnan(maxim):
        maxim = 0

    window_spec = Window \
        .orderBy("date")

    df_new = df_new. \
        withColumn("id_date", psf.row_number().over(window_spec) + maxim)

    df_new.show()

    df = df.union(df_new)

    df.show()

    sparkdb.write_table(df, "date_dim", "append")

    sparkdb.read_table("date_dim").show()


if __name__ == "__main__":

    sparkdb = SparkDB()

    # create_db(sparkdb.spark)

    seq = sparkdb.read_next_seq("date_dim")

    seq.show()

    # data = spark.range(0, 5)
    # data.write.format("delta").saveAsTable("data_dim")

    # spark.sql("select * from date_dim")
