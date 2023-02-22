from pyspark.sql import SparkSession
import pyspark.sql
import pyspark.sql.window
import pyspark.sql.functions as f
import delta
import utils

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

    def read_last_seq(self, table_name: str) -> int:

        seq = self.spark.table("sequences_cfg")

        seq = seq.where(f"table_name == '{table_name}'")

        last_seq = seq.pandas_api()["id"].iloc[0]

        return int(last_seq)

    def update_last_seq(self, df: pyspark.sql.dataframe, table_name: str):

        # Obtenemos la nueva ultima secuencia
        last_seq = df.pandas_api()["id"].max()

        # Actualizamos en la tabla de secuencias
        self.spark.sql(f"""
                    update sequences_cfg set id={last_seq} 
                    where table_name == '{table_name}'
                    """)

    def insert_id(self, df: pyspark.sql.dataframe, table_name: str) -> pyspark.sql.dataframe:
        """
         Inserta en df una columna 'id' con enteros consecutivos, desde la
         ultima secuencia que se entregó para la table_name.
        """

        # Ventana por cualquier columna, para poder usar row_number
        window_spec = pyspark.sql.window.Window \
            .orderBy(df.columns[0])

        # Obtenemos la ultima secuencia que se utilizó
        seq = self.read_last_seq(table_name)

        # Actualizamos la columna id con secuenciales desde la ultima secuencia
        df = df. \
            withColumn("id", f.row_number().over(window_spec) + seq)

        self.update_last_seq(df, table_name)

        return df
