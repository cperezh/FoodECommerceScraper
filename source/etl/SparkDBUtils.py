import utils
from pyspark.sql import SparkSession
import pyspark.sql
import delta


class SparkDB:

    __builder = SparkSession\
        .builder\
        .appName("FoodScraper with Delta") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
        .config("spark.sql.warehouse.dir","c:/tmp/spark-warehouse")\
        .config("javax.jdo.option.ConnectionURL","jdbc:derby:;databaseName=C:/tmp/metastore_db;create=true")\
        .enableHiveSupport()

    spark = delta.configure_spark_with_delta_pip(__builder)\
        .getOrCreate()

    def read_table(self, table_name: str) -> pyspark.sql.DataFrame:

        aaa = self.spark.sparkContext.getConf().get("spark.sql.warehouse.dir")

        df = self.spark.read.table(table_name)

        # Hago trim a los string que vienen de base de datos
        df = utils.trim_strings(df)

        return df

    @staticmethod
    def write_table(df: pyspark.sql.DataFrame, table_name: str, mode: str):

        # Saving data
        df.write \
            .format("delta") \
            .saveAsTable(table_name, mode=mode)

    def read_next_seq(self, table_name: str):

        seq = self.spark.table("sequences_cfg")

        seq.where(f"table_name == {table_name}")

        seq.show()

