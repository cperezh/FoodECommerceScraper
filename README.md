# FoodECommerceScraper

Repo for scraping food prices from eCommerce (Supermarket) in order to check inflation and
order uses.

## Scraper

### Authors:

- Oscar Promio opromio@uoc.edu
- Carlos Perez cperezha@uoc.edu

### Files

- /source : source code directory
  - main.py : executable module.
  - DiaScraper.py: Scraper code.
  - Utils.py: utilities library.
  - products_list.csv: cache file. Can be edited for debugging purposes. See "Usage" section.
- analysis.md : initial scraping analysis.
- sitemap.xml : sitemap file.
- requirements.txt : python libs.
- memoria.pdf: memoria del trabajo realizado.

### Usage

Recommended version is Python 3.8. Navigate source directory and Run:

    python -m main --reload_urls <reload>

To run start scraping. The process is divided in two steps:

1. Get a list of the products to scrape from online site and persist it on file.
2. Iterate the list and get every single product's properties and persist them on file.

`<reload>` parameter indicates wheter to use a previously chached products list file `products_list.csv`
or to scrape the products list file again from the online site. Can be `False` for using chached file or 
`True` to reload the list.

## ETL & BI

### Authors:

- Carlos Perez cperezh@gmail.com

### Source

- source/etl: source code directory
	- main.py: executable file
	- ETLFoodScraping.py: ETL code

### Usage

Run

	python -m source/etl/main
  
To install HADOOP, set the next variables:
	
    HADOOP_HOME=[..]\winutils\hadoop-3.0.0
    PATH=[..]\winutils\hadoop-3.0.0\bin
    PYSPARK_PYTHON=python
	
Download [Winutils:](https://github.com/steveloughran/winutils)