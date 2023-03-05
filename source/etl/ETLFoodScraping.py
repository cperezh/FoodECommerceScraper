import datetime

import pyspark.sql
from pyspark.sql.window import Window
import pyspark.sql.functions as psf
from pyspark.sql.types import StructType, StructField, DateType, \
    StringType, FloatType, TimestampType
from SparkDBUtils import SparkDB
import pandas as pd
import logging
from delta import DeltaTable


@psf.pandas_udf(StringType())
def split_categoria(categorie_col: pd.Series) -> pd.Series:
    """
    Extrae el primer elemento de la lista de categorias
    """

    salida = categorie_col.apply(lambda x: eval(x)[0])

    return salida


class ETLFoodScraping:
    """
    Execute run method to run ETL procecess
    """

    simple_schema = StructType([
        StructField("date", DateType(), True),
        StructField("product", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", FloatType(), True),
        StructField("categories", StringType(), True),
        StructField("unit_price", FloatType(), True),
        StructField("units", StringType(), True),
        StructField("discount", FloatType(), True),
        StructField("ts_load", TimestampType(), True)
    ])

    def __init__(self):
        self.sparkDB = SparkDB()

    def update_date_dim(self, dataset: pyspark.sql.DataFrame):

        logging.getLogger(__name__).info("Start update_date_dim")

        # obtenemos las nuevas fechas del fichero
        date_dim_new = dataset.select([dataset.date]).distinct()

        # Obtenemos las fechas en la base de datos
        date_dim = self.sparkDB.read_table("date_dim").select("date").distinct()

        # Vemos cuál de las nuevas no está en la base de datos
        data_dim_merge = date_dim_new.exceptAll(date_dim)

        # Incluimos el Timestamp y el año
        data_dim_merge = data_dim_merge\
            .withColumn("ts_load", psf.current_timestamp())\
            .withColumn("year", psf.year("date"))

        logging.getLogger(__name__).info("Fechas actualizadas: " + str(data_dim_merge.count()))

        # Actializamos la base de datos.
        self.sparkDB.write_table(data_dim_merge, "date_dim", "append", "id_date")

    def update_producto_dim(self, dataset: pyspark.sql.DataFrame, year: int):

        logging.getLogger(__name__).info("Start update_producto_dim")

        # Ventana para obtener la ultima version de cada producto
        window_spec = Window\
            .partitionBy("product_id")\
            .orderBy(psf.col("date").desc())

        # Nos quedamos con la ultima version de cada producto en el dataset,
        # ya que se repiten en todas las fechas
        product_dim_new = dataset\
            .withColumn("row_number", psf.row_number().over(window_spec)) \
            .where("row_number = 1") \
            .withColumn("categoria", split_categoria(dataset.categories)) \
            .select(["product_id",
                     "product",
                     "units",
                     "brand",
                     "categories",
                     "categoria",
                     "date"])

        # Obtenemos os productos de base de datos
        product_dim_db = DeltaTable.forName(self.sparkDB.spark, "producto_dim")\
            .select(["product_id",
                     "product",
                     "units",
                     "brand",
                     "categories",
                     "categoria",
                     "date"])

        # Obtenemos los productos con diferencias, comparando base de datos con dataset
        # p_merge = product_dim_new.exceptAll(product_dim_db)


        # Añadimos fecha de carga
        p_merge = p_merge\
            .withColumn("ts_load", psf.current_timestamp())\

        logging.getLogger(__name__).info("Productos actualizados: " + str(p_merge.count()))

        self.sparkDB.write_table(p_merge, "producto_dim", "append", "id_producto")

    def update_producto_dia_fact(self,  dataset: pyspark.sql.DataFrame, year: int):

        logging.getLogger(__name__).info("Start update_producto_dia_fact")

        # Ventana para obtener la ultima version de cada producto
        window_spec = Window \
            .partitionBy(["product_id"]) \
            .orderBy(psf.col("date").desc())

        product_dim_db = self.sparkDB.read_table("producto_dim") \
            .withColumn("row_number", psf.row_number().over(window_spec)) \
            .where("row_number = 1") \
            .select(["product_id",
                     "id_producto"])\

        # tabla de fechas para hacer el lookup
        date_dim_db = self.sparkDB.read_table("date_dim").select(["date", "id_date", "year"])

        # obtengo los hechos del fichero
        producto_dia_fact_new = dataset\
            .join(product_dim_db.alias("p"), "product_id", "left")\
            .join(date_dim_db.alias("d"), "date", "left")\
            .select(["price",
                     "unit_price",
                     "discount",
                     "id_producto",
                     "id_date",
                     "d.year"])

        # Obtengo los hechos de base de datos
        producto_dia_fact_db = self.sparkDB.read_table("producto_dia_fact")\
            .select(["price",
                     "unit_price",
                     "discount",
                     "id_producto",
                     "id_date",
                     "year"])

        # Obtenemos los hechos nuevos, comparando base de datos con dataset
        producto_dia_fact_merge = producto_dia_fact_new.exceptAll(producto_dia_fact_db)

        # Añadimos fecha de carga y el año de particionado
        producto_dia_fact_merge = producto_dia_fact_merge\
            .withColumn("ts_load", psf.current_timestamp())\

        logging.getLogger(__name__).info("Hechos actualizados: " + str(producto_dia_fact_merge.count()))

        self.sparkDB.write_table(producto_dia_fact_merge, "producto_dia_fact", "append")

    def update_precio_dia_norm_fact(self):

        logging.getLogger(__name__).info("Start update_precio_dia_norm_fact")

        producto_dia_fact = self.sparkDB.read_table("producto_dia_fact")

        # Agrupo a nivel de DIA, para calcular el total de precios diarios
        price_dia_agg_fact = producto_dia_fact\
            .groupby("id_date")\
            .agg(psf.sum("price").alias("sum_price"),
                 psf.sum("unit_price").alias("sum_unit_price"),
                 psf.count("id_producto").alias("num_products"))

        # Creo columns de precio diario ponderado por el numero de productos del dia
        price_dia_agg_fact = price_dia_agg_fact \
            .withColumn("sum_price_ponderado", psf.col("sum_price") / psf.col("num_products")) \
            .withColumn("sum_unit_price_ponderado", psf.col("sum_unit_price") / psf.col("num_products"))

        pdf_pandas = price_dia_agg_fact.pandas_api()

        # Aplico normalización min-max para poder comparar los precios
        precio_dia_agg_norm_fact = price_dia_agg_fact \
            .withColumn("sum_price_norm",
                        (psf.col("sum_price_ponderado") - pdf_pandas["sum_price_ponderado"].min()) /
                        (pdf_pandas["sum_price_ponderado"].max() - pdf_pandas["sum_price_ponderado"].min())) \
            .withColumn("sum_unit_price_norm",
                        (psf.col("sum_unit_price_ponderado") - pdf_pandas["sum_unit_price_ponderado"].min()) /
                        (pdf_pandas["sum_unit_price_ponderado"].max() - pdf_pandas["sum_unit_price_ponderado"].min()))

        self.sparkDB.write_table(precio_dia_agg_norm_fact, "precio_dia_agg_norm_fact", "overwrite")

        logging.getLogger(__name__).info("precio_dia_agg_norm_fact creada")

    def read(self) -> pyspark.sql.dataframe:

        logging.getLogger(__name__).info("Read dataset")

        dataset = self.sparkDB.spark.read.option("delimiter", ";") \
            .csv("../../dataset/dataset.csv", schema=self.simple_schema, header=True)

        return dataset

    def load_dwh(self, dataset: pyspark.sql.dataframe):

        logging.getLogger(__name__).info("Start Load DWH")

        year = datetime.date.today().year

        self.update_date_dim(dataset)

        self.update_producto_dim(dataset)

        self.update_producto_dia_fact(dataset, year)

        self.update_precio_dia_norm_fact()

    def export_dwh(self):

        logging.getLogger(__name__).info("Start export_dwh")

        self.sparkDB.spark.table("date_dim")\
            .toPandas()\
            .to_csv("c:/tmp/extract/date_dim.csv",
                    index=False, sep=";", decimal=",")

        self.sparkDB.spark.table("producto_dim") \
            .toPandas() \
            .to_csv("c:/tmp/extract/producto_dim.csv",
                    index=False, sep=";", decimal=",")

        self.sparkDB.spark.table("producto_dia_fact") \
            .toPandas() \
            .to_csv("c:/tmp/extract/producto_dia_fact.csv",
                    index=False, sep=";", decimal=",")

        self.sparkDB.spark.table("precio_dia_agg_norm_fact") \
            .toPandas() \
            .to_csv("c:/tmp/extract/precio_dia_agg_norm_fact.csv",
                    index=False, sep=";", decimal=",")

    def run(self):

        logging.getLogger(__name__).info("Comienza ETL FoodScraping")

        dataset = self.read()

        self.load_dwh(dataset)

        self.export_dwh()

        logging.getLogger(__name__).info("Finaliza ETL FoodScraping: SUCCESS")