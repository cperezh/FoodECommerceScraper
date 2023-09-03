
from pyspark.sql.types import StructType, StructField, DateType, \
    StringType, FloatType, DoubleType, TimestampType

import pyspark.sql


class Producto:

    producto_dim_schema = StructType([
        StructField("product", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("categories", StringType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("units", StringType(), True),
        StructField("discount", DoubleType(), True),
        StructField("date", DateType(), True),
        StructField("ts_load", TimestampType(), True)
    ])

    def __init__(self):

        self.product_id = ""
        self.price = 0
        self.product = ""
        self.brand = ""
        self.unit_price = 0
        self.units = 0
        self.categories = ""
        self.discount = 0
        self.date = None
        self.ts_load = None

    def to_dict(self) -> dict:
        return self.__dict__

    def to_spark_df(self, spark: pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:

        producto_dict = (self,)

        df = spark.createDataFrame(data=producto_dict, schema=Producto.producto_dim_schema)

        return df



