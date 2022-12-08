import pyspark.sql.dataframe
from pyspark.sql.types import StringType
import pyspark.sql.functions as psf


def trim_strings(df: pyspark.sql.dataframe) -> pyspark.sql.dataframe:

    # Trim los strings
    for c in df.columns:
        if type(df.schema[c].dataType) == StringType:
            df = df.withColumn(c, psf.trim(df[c]))

    return df
