import utils
import sys
from Producto import Producto
import re as re
import SparkDBUtils
import os
import logging
import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import delta
import pyspark.sql.functions as F
from utils import ProductoIncorrectoException
from requests.exceptions import ConnectionError

class DiaScraper:
    """
    Objeto para descargar información de la pagina de DIA
    """

    URLSiteMap = '/sitemap.xml'
    URLSite = 'https://www.dia.es'
    URLCompreOnline = 'https://www.dia.es'

    LOG_FILE = os.path.join('.', 'logs', 'logs.log')

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                        handlers=[
                            logging.FileHandler(LOG_FILE),
                            logging.StreamHandler(sys.stdout)
                        ])

    sparkDB = SparkDBUtils.SparkDB()

    staging_product_schema = StructType([
        StructField("id_producto", StringType(), True),
        StructField("url_product", StringType(), True),
        StructField("index", IntegerType(), True)
    ])

    def __init__(self):
        self.data_path = utils.create_data_folder()
        self.execution_datetime = datetime.datetime.now()

    def __cargar_paginas_producto(self):
        """
        Carga las paginas de producto que haya en el sitemap en la
        varible 'listaPaginasProducto'
        :return:
        """

        # TODO Borrar todos los archivos temp

        sitemap = utils.get_xml_page(self.URLSite + self.URLSiteMap)

        paginas_producto = sitemap.find_all("loc", string=re.compile('.+/p/\\d+'))

        pattern = re.compile("\\d.*")

        lista_paginas_producto = []

        for index, p in enumerate(paginas_producto):

            id_product = str(pattern.search(p.string).group())

            url = str(p.string)

            lista_paginas_producto.append((id_product, url, index))

        df = self.sparkDB.spark.createDataFrame(data=lista_paginas_producto,
                                                schema=DiaScraper.staging_product_schema)

        self.sparkDB.write_table(df, "producto_dia.staging_product", "overwrite", None)

    def start_scraping(self, reload: bool = False):
        """
        Funciona principal que realiza el proceso de scraping. En funcion del parámetro reload
        se recarga de nuevo la lista de urls de productos a partir del site de DIA o se utiliza
        la caché de fichero.

        :param reload: True si se recarga la caché o False si se utiliza la actual.
        :return:
        """

        if reload:
            self.__cargar_paginas_producto()

        # Leer paginas de producto de la tabla de staging, ordenadas por index
        df_staging_product = self.sparkDB.spark\
            .table("producto_dia.staging_product")\
            .orderBy("index")\
            .toPandas()

        number_products_scan = len(df_staging_product)
        elementos_tratados = 0
        lista_productos = []

        # Recorro todos los productos de la tabla de staging
        for i, stg_producto in df_staging_product.iterrows():

            logging.info(f"Number of products left: {number_products_scan - elementos_tratados}")

            logging.info(f" Scraping: product_id: {stg_producto['id_producto']} : {stg_producto['url_product']}")

            try:
                producto = utils.get_info_from_url(stg_producto['url_product'])

                producto.ts_load = datetime.datetime.now()

                lista_productos.append(producto)

            except ProductoIncorrectoException as e:
                logging.warning(f"!!!Failed. Exception: {e}")

            except ConnectionError as e:
                logging.warning(f"!!!ConnectionError. Exception: {e}")
                break

            # Actualización de punteros
            elementos_tratados += 1

            # Cada 100 elementos o cuando ya no queden elementos por tratar:
            # insertamos los elementos tratados y purgamos la tabla de staging
            if number_products_scan == elementos_tratados or elementos_tratados % 500 == 0:

                pdf = Producto.list_to_spark_df(lista_productos, self.sparkDB.spark)

                self.sparkDB.write_table(pdf, "producto_dia.producto_dim", "append")

                lista_productos = []

                dt = delta.DeltaTable.forName(self.sparkDB.spark, "producto_dia.staging_product")

                dt.delete(F.col("index") <= stg_producto['index'])

                logging.warning(f">>>>> Borrando productos con indice menor que  {stg_producto['index']} .")

        return

    def insertar_url_pruebas(self):

        producto = [(258603, "https://www.dia.es/agua-refrescos-y-zumos/cola/p/258603",0)]

        df = self.sparkDB.spark.createDataFrame(producto, schema=DiaScraper.staging_product_schema)

        self.sparkDB.write_table(df, "producto_dia.staging_product", mode="overwrite")


if __name__ == "__main__":
    diaScraper = DiaScraper()

    diaScraper.insertar_url_pruebas()



