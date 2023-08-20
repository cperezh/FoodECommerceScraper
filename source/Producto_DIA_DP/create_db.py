from SparkDBUtils import SparkDB
from pyspark.sql.types import DateType, StructType, StructField, IntegerType, TimestampType
import datetime as dt


def create_db(spark):

    spark.sql("CREATE SCHEMA IF NOT EXISTS producto_dia")

    sequences_cfg = """
            CREATE OR REPLACE TABLE producto_dia.sequences_cfg
            (
                table_name STRING,
                id BIGINT,
                ts_load timestamp
            ) USING DELTA
        """

    spark.sql(sequences_cfg)

    product_dim = """
            CREATE OR REPLACE TABLE producto_dia.producto_dim
            (
                id_producto int,
                product string,
                brand string,
                categories string,
                product_id string,
                date date,
                categoria string,
                units string,
                ts_load timestamp
            ) USING DELTA;
            """

    spark.sql(product_dim)

    conf = f"""
            INSERT INTO producto_dia.sequences_cfg VALUES('producto_dim',0,'{dt.datetime.now()}')
        """

    spark.sql(conf)

    product_dim = """
                CREATE OR REPLACE TABLE producto_dia.staging_product
                (
                    id_producto string,
                    url_product string,
                    fecha_datos timestamp
                ) USING DELTA;
                """

    spark.sql(product_dim)


if __name__ == "__main__":

    sparkdb = SparkDB()

    create_db(sparkdb.spark)
