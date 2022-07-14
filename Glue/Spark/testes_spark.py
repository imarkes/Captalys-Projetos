import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
data = [("James", "Smith", "USA", "CA"),
        ("Michael", "Rose", "USA", "NY"),
        ("Robert", "Williams", "USA", "CA"),
        ("Maria", "Jones", "USA", "FL")
        ]
columns = [" first name", "last/name", "  country     ", "state     "]
df = spark.createDataFrame(data=data, schema=columns)


rename_cols = {c:c.strip().replace(' ', '_').replace('/', '_') for c in df.columns}
df=df.select([when(col(c)=="",None).otherwise(col(c)).alias(rename_cols.get(c, c)) for c in df.columns])

# replacements = {c:c.replace('[\\r\\n]', '').strip() for c in df.columns}
# df = df.select([col(c).alias(replacements.get(c, c)) for c in df.columns])
df.show(truncate=False)

# udf_valor_credito = f.udf(lambda x: set_proposta_valor_credito(x))
# propostas_v1 = propostas_v1.withColumn('valor_credito', udf_valor_credito(f.col("valor_credito")))
