import pyspark.sql
import utils
from pyspark.sql import SparkSession


class SparkDBUtils:

    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.jars", "C:\\Users\\Carlos\\Proyectos\\FoodECommerceScraper\\lib\\postgresql-42.5.1.jar") \
        .getOrCreate()

    def read_table(self, table_name: str) -> pyspark.sql.DataFrame:
        df = self.spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/FoodScraping") \
            .option("dbtable", table_name) \
            .option("user", "postgres") \
            .option("password", "postgres") \
            .option("driver", "org.postgresql.Driver") \
            .load()

        # Hago trim a los string que vienen de base de datos
        df = utils.trim_strings(df)

        return df

    @staticmethod
    def write_table(df: pyspark.sql.DataFrame, table_name: str, mode: str):

        # Saving data to a JDBC source
        df.write \
            .format("jdbc") \
            .mode("error") \
            .option("url", "jdbc:postgresql://localhost:5432/FoodScraping") \
            .option("dbtable", table_name) \
            .option("user", "postgres") \
            .option("password", "postgres") \
            .option("driver", "org.postgresql.Driver") \
            .save(mode=mode)

