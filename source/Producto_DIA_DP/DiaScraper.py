import utils
import hashlib
import glob
import shutil
import sys
import pandas as pd
import json
from Producto import Producto
import re as re
import SparkDBUtils
import os
import logging
import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import delta
import pyspark.sql.functions as F


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

    def __save_record(self, record: Producto, filename: str):
        """
        saves scrapped information in temp folder.
        """
        with open(os.path.join(self.data_path, 'tmp', hashlib.md5(filename.encode()).hexdigest() + '.json'), 'w+',
                  encoding='utf-8') as f:
            json.dump(record.to_dict(), f, ensure_ascii=False)

    def __save_results(self):
        """
        aggregates temp information in a single csv from the execution
        """
        json_output = {"data": []}
        logging.info("Crawling finished. Processing tmp data.")
        # para cada archivo que encontramos en la carpeta tmp lo guardamos en memoria.
        for file in glob.glob(os.path.join(self.data_path, 'tmp', '*.json')):
            with open(file, 'r+', encoding='utf-8') as f:
                record = json.loads(f.read())
                json_output["data"].append(record)

        # guardamos la lista total en csv.
        pd.DataFrame(json_output["data"]) \
            .to_csv(os.path.join(self.data_path,
                                 datetime.datetime.strftime(self.execution_datetime,
                                                            '%Y%m%d_%H%M') + '_dia.csv'),
                    sep=";", encoding="utf-8", index=False)
        # eliminamos el directorio tmp
        shutil.rmtree(os.path.join(self.data_path, 'tmp'))

    def generate_dataset(self):
        """
        Appends or generates the dataset.csv file with the newly scrapped information.
        """
        try:
            dataset = pd.read_csv(os.path.join(self.data_path, '..', 'dataset.csv'), sep=";", encoding="utf-8")
        # si no existe o no contiene datos, generamos un nuevo dataset
        except (FileNotFoundError, pd.errors.EmptyDataError):
            dataset = pd.DataFrame()
        # para cada csv que encuentra de ejecuciones pasadas lo concatena al archivo dataset
        for result in glob.glob(os.path.join(self.data_path, '../*/*.csv')):
            data = pd.read_csv(result, sep=";", encoding="utf-8")
            dataset = pd.concat([dataset, data])
        # eliminamos duplicados
        dataset.drop_duplicates(inplace=True)
        dataset.to_csv(os.path.join(self.data_path, '..', 'dataset.csv'), sep=";", encoding="utf-8", index=False)

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
            .pandas_api()

        number_products_scan = len(df_staging_product)
        elementos_tratados = 0

        # Recorro todos los productos de la tabla de staging
        for i, producto in df_staging_product.iterrows():

            product_number = producto['id_producto']
            product_url = producto['url_product']
            index = producto["index"]

            logging.info(f"Number of products left: {number_products_scan - elementos_tratados}")

            logging.info(f"Crawling {product_url}")
            record = utils.get_info_from_url(product_url)
            logging.info(f"Scanned: product_id: {product_number}")
            try:
                # TODO: Este metodo debe escribir en la tabla final
                self.__save_record(record, record.product)

            except AttributeError:
                logging.warning(f"{product_url} failed. No information retrieved.")

            # Actualización de punteros
            elementos_tratados += 1

            # Cada 100 elementos, purgamos la tabla de staging o cuando ya no queden elementos por tratar
            if number_products_scan == elementos_tratados or elementos_tratados % 8 == 0:
                dt = delta.DeltaTable.forName(self.sparkDB.spark, "producto_dia.staging_product")
                dt.delete(F.col("index") <= index)
                logging.warning(f"Borrando productos con indice menor que  {index} .")

        self.__save_results()
        return
