from pyspark.context import SparkContext
from awsglue.context import GlueContext

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

df = spark.createDataFrame([("Jeffery", 28), ("Kwame", 34)], ["name", "age"])
df.printSchema()
df.show()