from ETLFoodScraping import ETLFoodScraping
import logging


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)

    ETLFoodScraping().run()
