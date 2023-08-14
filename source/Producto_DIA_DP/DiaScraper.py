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


class DiaScraper:
    """
    Objeto para descargar información de la pagina de DIA
    """

    URLSiteMap = '/sitemap.xml'
    URLSite = 'https://www.dia.es'
    URLCompreOnline = 'https://www.dia.es'

    PRODUCTS_CSV_FILE = "products_list.csv"
    LOG_FILE = os.path.join('.', 'logs', 'logs.log')

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                        handlers=[
                            logging.FileHandler(LOG_FILE),
                            logging.StreamHandler(sys.stdout)
                        ])

    sparkDB = SparkDBUtils.SparkDB()

    def __init__(self):

        # Lista de paginas de producto del site
        self.listaPaginasProducto = []
        self.data_path = utils.create_data_folder()
        self.execution_datetime = datetime.datetime.now()

    def __cargar_paginas_producto(self):
        """
        Carga las paginas de producto que haya en el sitemap en la
        varible 'listaPaginasProducto'
        :return:
        """

        sitemap = utils.__get_xml_page(self.URLSite + self.URLSiteMap)

        paginas_producto = sitemap.find_all("loc", string=re.compile('.+/p/\\d+'))

        pattern = re.compile("\\d+")

        for p in paginas_producto:

            id_product = pattern.search(p.string).group()

            self.listaPaginasProducto.append(p.string)

    def __cargar_paginas_producto_autonomo_con_opcion(self, reload: bool):
        """
        Carga las paginas de producto con navegacion autonomo.

        :param reload: True si geremos volver a lanzar el escaneo online.
            False si queremos cargar los productos del fichero csv cacheado (PRODUCTS_CSV_FILE))
        """

        if reload:
            logging.info("Recargando URLs de productos...")
            self.__cargar_paginas_producto()

        self.__read_products_from_csv()

    def __read_products_from_csv(self):
        with open(self.PRODUCTS_CSV_FILE, "r") as f:
            for url in f:
                self.listaPaginasProducto.append(url.strip())

    def __write_products_to_csv(self, product_list: list):
        with open(self.PRODUCTS_CSV_FILE, "w") as f:
            for url in product_list:
                f.write(url + "\n")

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

        # TODO: Leer paginas de producto

        logging.info("Number of products to scan: " + str(len(self.listaPaginasProducto)))

        for product_number, product_url in self.listaPaginasProducto:

            logging.info(f"Crawling {product_url}")
            record = utils.get_info_from_url(product_url)
            logging.info(f"Scanned: {product_number + 1} - product_id: {record.product_id}")
            try:
                self.__save_record(record, record.product)

                # TODO: Borrar el producto de la tabla

            except AttributeError:
                logging.warning(f"{product_url} failed. No information retrieved.")

        self.__save_results()
        return
