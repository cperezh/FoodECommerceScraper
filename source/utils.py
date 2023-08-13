import re
import datetime
import os


def create_data_folder():
    today = str(datetime.date.today()).replace('-', '')
    data_path = os.path.join(os.getcwd(), '..', 'dataset', today)
    os.makedirs(os.path.join(data_path, 'tmp'), exist_ok=True)
    return data_path


def preprocess_str(text: str) -> str:
    rm_chars = ["\r", "\n", "\t"]
    for char in rm_chars:
        text = text.replace(char, "")
    return text.replace(",", ".").strip()


def process_unit_price(text: str) -> str:
    match = re.search('\\€.+$', text).group().strip()
    return match


def process_price(text: str) -> str:
    match = re.search('\\d+,\\d+', text).group().strip()
    return match.replace(",", ".")


def process_discount(text: str) -> str:
    match = re.search('\\b\\d+\\%', text).group().strip()
    return match.replace(",", ".")


def process_brand(text: str) -> str:
    text = preprocess_str(text)
    match = re.findall('[A-Z]\\w+', text)

    return match[1]


def process_name(text: str) -> str:
    text = preprocess_str(text)
    match = re.findall('[A-Z][a-z áéíóú]+', text)

    return match[0]


