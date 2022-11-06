from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,StructField,IntegerType

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("EmptyDataFrame").getOrCreate()

    #Empty RDD
    header = ["name","gender"]
    input_rdd = spark.sparkContext.emptyRDD()
    #print(input_rdd.getNumPartitions())
    print(f"number of partitions : {input_rdd.getNumPartitions()}")

    #custom Schema
    schema_data = StructType([
    StructField("id",IntegerType(),False),
    StructField("name",StringType())
        ])

    df = spark.createDataFrame(input_rdd,schema_data)
    print(df.printSchema())
