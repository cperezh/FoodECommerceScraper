
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
        self.product = ""
        self.product_id = ""
        self.brand = ""
        self.price = 0
        self.categories = ""
        self.unit_price = 0
        self.units = 0
        self.discount = 0
        self.date = None
        self.ts_load = None

    def to_dict(self) -> dict:
        return self.__dict__

    def to_tuple(self) -> tuple:
        t = (self.product,
             self.product_id,
             self.brand,
             self.price,
             self.categories,
             self.unit_price,
             self.units,
             self.discount,
             self.date,
             self.ts_load)

        return t

    def to_spark_df(self, spark: pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:

        producto_dict = (self,)

        df = spark.createDataFrame(data=producto_dict, schema=Producto.producto_dim_schema)

        return df

    @staticmethod
    def list_to_spark_df(producto_list: list, spark: pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:

        lista_dict = [producto.to_tuple for producto in producto_list ]

        df = spark.createDataFrame(data=lista_dict, schema=Producto.producto_dim_schema)

        return df


if __name__ == "__main__":
    p1 = Producto()
    p2 = Producto()

    lista = [p1, p2]

    a = [(tuple(lista))]

    print(a)


