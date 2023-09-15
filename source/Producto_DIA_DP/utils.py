import re
import datetime
from bs4 import BeautifulSoup
import requests
import time
import logging
from typing import Tuple, List
import os
from Producto import Producto

HEADERS = {
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/107.0.0.0 Safari/537.36 '
    }

execution_datetime = datetime.datetime.now()


def get_xml_page(url: str) -> BeautifulSoup:
    """
    :param url:
    :return: Devuelve un objeto BeautifulSoup para operar con la pagina cargada
    """

    session = requests.Session()
    page = session.get(url, headers=HEADERS)
    soup = BeautifulSoup(page.content, features='xml')

    return soup


def get_html_page(url: str) -> BeautifulSoup:
    """
    :param url:
    :return: Devuelve un objeto BeautifulSoup para operar con la pagina cargada
    """

    session = requests.Session()
    # Se simula navegacion humana, con retraso de 10x el tiempo del request.
    t0 = time.time()
    page = session.get(url, headers=HEADERS)
    delay = time.time() - t0
    time.sleep(0.2 * delay)
    soup = BeautifulSoup(page.content, features='html.parser')

    return soup


def get_info_from_url(url: str) -> Producto:
    """
    param url: url address to scrap
    return: dic with scrapped information.
    raise ProductoIncorrectoException: when coudn't fetch any product information
    """

    page = get_html_page(url)

    producto = Producto()

    producto.product_id = str(url.split('/')[-1])
    producto.price = __obtain_price(page)
    producto.product, producto.brand = __obtain_name(page)
    producto.unit_price, producto.units = __obtain_price_per_unit(page)
    producto.categories = __obtain_categories(page)
    producto.discount = __obtain_discount(page)
    producto.date = execution_datetime.date()

    # comprobamos si hay informacion missing.
    if any([producto.price is None, producto.product is None, producto.brand is None,
            producto.unit_price is None, producto.units is None]):
        logging.warning(f"{url} failed. Missing information.")
        raise ProductoIncorrectoException(f"Producto_id: {producto.product_id}")

    return producto


def __obtain_name(page: BeautifulSoup) -> Tuple[str, str]:
    fetched_product = page.find_all("h1", class_="product-title")
    try:
        product_name = [process_name(product.text) for product in fetched_product][0]
        brand = [__process_brand(product.text) for product in fetched_product][0]
    except (IndexError, AttributeError):
        logging.warning('Product name not found')
        product_name = None
        brand = None
    return product_name, brand


def __obtain_price(page: BeautifulSoup) -> float:
    try:
        fetched_price = page.find_all("p", class_="buy-box__active-price")
        price = float([__process_price(price.text) for price in fetched_price][0])
    except (IndexError, AttributeError):
        logging.warning('Product price not found')
        price = None
    return price


def __obtain_categories(page: BeautifulSoup) -> List[str]:
    fetched_categories = page.find_all("span", class_="breadcrumb-item__link")
    try:
        categories = [__preprocess_str(category.text) for category in fetched_categories]
    except AttributeError:
        categories = None
    return categories


def __obtain_price_per_unit(page: BeautifulSoup) -> Tuple[float, str]:
    fetched_unit_prices = page.find_all("p", "buy-box__price-per-unit")
    try:
        price = float([__process_price(unit_price.text) for unit_price in fetched_unit_prices][0])
        units = [__process_unit_price(unit_price.text) for unit_price in fetched_unit_prices][0]
    except (IndexError, AttributeError):
        logging.warning('Unit price not found')
        price = None
        units = None
    return price, units


def __obtain_discount(page: BeautifulSoup) -> str:
    try:
        fetched_discount = page.find_all("span", "product_details_promotion_description")
        discount_percentage = [__process_discount(discount.text) for discount in fetched_discount][0]
    except (IndexError, AttributeError):
        discount_percentage = None
    return discount_percentage


def create_data_folder():
    today = str(datetime.date.today()).replace('-', '')
    data_path = os.path.join(os.getcwd(), '../..', 'dataset', today)
    os.makedirs(os.path.join(data_path, 'tmp'), exist_ok=True)
    return data_path


def __preprocess_str(text: str) -> str:
    rm_chars = ["\r", "\n", "\t"]
    for char in rm_chars:
        text = text.replace(char, "")
    return text.replace(",", ".").strip()


def __process_unit_price(text: str) -> str:
    match = re.search('€.+$', text).group().strip()
    return match


def __process_price(text: str) -> str:
    match = re.search('\\d+,\\d+', text).group().strip()
    return match.replace(",", ".")


def __process_discount(text: str) -> str:
    match = re.search('\\b\\d+%', text).group().strip()
    return match.replace(",", ".")


def __process_brand(text: str) -> str:
    text = __preprocess_str(text)
    match = re.findall('[A-Z]\\w+', text)

    return match[1]


def process_name(text: str) -> str:
    text = __preprocess_str(text)
    match = re.findall('[A-Z][a-z áéíóú]+', text)

    return match[0]


def __print_page(page: BeautifulSoup, ruta: str):
    """
    imprime la pagina escrapeada en la ruta correspondiente.
    """
    with open(ruta, "w", encoding="utf-8") as f:
        f.write(page.prettify())


class ProductoIncorrectoException(Exception):
    ...
