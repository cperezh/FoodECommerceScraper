import pyspark.sql
from pyspark.sql.window import Window
import pyspark.sql.functions as psf
from pyspark.sql.types import StructType, StructField, DateType, \
    StringType, FloatType, TimestampType, IntegerType
from SparkDBUtils import SparkDBUtils


class ETLFoodScraping:

    def __init__(self):
        self.sparkDB = SparkDBUtils()

    def update_date_dim(self, dataset: pyspark.sql.DataFrame):
        # obtenemos las nuevas fechas del fichero
        date_dim_new = dataset.select([dataset.date]).distinct()

        # Obtenemos las fechas en la base de datos
        date_dim = self.sparkDB.read_table("date_dim").select("date").distinct()

        # Vemos cuál de las nuevas no está en la base de datos
        data_dim_merge = date_dim_new.exceptAll(date_dim)

        # Incluimos el Timestamp
        data_dim_merge = data_dim_merge.withColumn("ts_load", psf.current_timestamp())

        # Actializamos la base de datos.
        self.sparkDB.write_table(data_dim_merge, "date_dim", "append")

        print("Fechas actualizadas: "+str(data_dim_merge.count()))

    def update_producto_dim(self, dataset: pyspark.sql.DataFrame):

        # Obtenemos los productos del fichero
        product_dim_new = dataset.select([dataset.product_id,
                                          dataset.product,
                                          dataset.brand,
                                          dataset.categories]).distinct()

        # Obtenemos os productos de base de datos
        product_dim_db = self.sparkDB.read_table("producto_dim")\
            .select(["product_id",
                     "product",
                     "brand",
                     "categories"])

        # Trim los strings
        for c in product_dim_db.columns:
            if type(product_dim_db.schema[c].dataType) == StringType:
                product_dim_db = product_dim_db.withColumn(c, psf.trim(product_dim_db[c]))

        # Obtenemos los productos nuevos
        product_dim_merge = product_dim_new.exceptAll(product_dim_db)

        # Añadimos fecha de carga
        product_dim_merge = product_dim_merge.withColumn("ts_load", psf.current_timestamp())

        print("Productos actualizados: ", product_dim_merge.count())

        self.sparkDB.write_table(product_dim_merge, "producto_dim", "append")


    def update_producto_dia_fact(self,  dataset: pyspark.sql.DataFrame):

        # obtengo los hechos del fichero
        producto_dia_fact = dataset.select(["price",
                                            "unit_price",
                                            "unit_price",
                                            "discount",
                                            "product_id",
                                            "date"]).distinct()

        # Busco el id_producto
        product_dim_db = self.sparkDB.read_table("producto_dim")

        window_spec = Window.partitionBy("product_id").orderBy(psf.col("ts_load").desc())

        product_dim_db = product_dim_db\
            .withColumn("row_number", psf.row_number().over(window_spec))\
            .where("row_number = 1")\
            .join(product_dim_db, on="id_producto")

        #    .where("row_number==1")

        product_dim_db.show(10)

        #    .select(["product_id","id_producto"])


        #producto_dia_fact["product_id"] == product_dim["product_id"], "inner")

        producto_dia_fact.show(3)




    def run(self):
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

        dataset = self.sparkDB.spark.read.option("delimiter", ";") \
            .csv("C:\\Users\\Carlos\\Proyectos\\FoodECommerceScraper\\dataset\\dataset.csv",
                 schema=simple_schema, header=True)

        self.update_date_dim(dataset)

        self.update_producto_dim(dataset)

        # self.update_producto_dia_fact(dataset)
