from SparkDBUtils import SparkDB
from pyspark.sql.types import DateType, StructType, StructField, IntegerType, TimestampType
import datetime as dt


def create_db(spark):

    spark.sql("CREATE SCHEMA IF NOT EXISTS analisis_precios")

    sequences_cfg = """
            CREATE OR REPLACE TABLE analisis_precios.sequences_cfg
            (
                table_name STRING,
                id BIGINT,
                ts_load timestamp
            ) USING DELTA
        """

    spark.sql(sequences_cfg)

    date_dim = """
        CREATE OR REPLACE TABLE analisis_precios.date_dim
        (
            id_date int,
            date date,
            year int,
            ts_load timestamp
        ) USING DELTA;
        """

    spark.sql(date_dim)

    conf = f"""
        INSERT INTO analisis_precios.sequences_cfg VALUES('analisis_precios.date_dim',0,'{dt.datetime.now()}')
    """

    spark.sql(conf)

    product_dim = """
            CREATE OR REPLACE TABLE analisis_precios.producto_dim
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
            INSERT INTO analisis_precios.sequences_cfg VALUES('analisis_precios.producto_dim',0,'{dt.datetime.now()}')
        """

    spark.sql(conf)

    producto_dia_fact = """
                CREATE OR REPLACE TABLE analisis_precios.producto_dia_fact
                (
                    id_producto int,
                    id_date int,
                    price double,
                    unit_price double,
                    discount double,
                    year int,
                    ts_load timestamp
                ) USING DELTA
                PARTITIONED BY (year)
                """

    spark.sql(producto_dia_fact)

    precio_dia_agg_norm_fact = """
                    CREATE OR REPLACE TABLE analisis_precios.precio_dia_agg_norm_fact
                    (
                        id_date int,
                        sum_price double,
                        sum_unit_price double,
                        num_products long,
                        sum_price_ponderado double,
                        sum_unit_price_ponderado double,
                        sum_price_norm double,
                        sum_unit_price_norm double,
                        ts_load timestamp
                    ) USING DELTA;
                    """

    spark.sql(precio_dia_agg_norm_fact)


if __name__ == "__main__":

    sparkdb = SparkDB()

    create_db(sparkdb.spark)
