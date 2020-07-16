import re

from pyspark.sql.types import Row
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc=spark.sparkContext

df = spark.read.csv("data/Employees_Position_Salaries.csv")

'''reading our csv file, we can check if it has been correctly
 readed with df.printSchema()'''

'''then it is parsed to a row object'''

def parse(file):
    return Row(
        name = file[0],
        position= file[1],
        department= file[2],
        salary= file[3])
parsed=df.rdd.map(parse)
print(parsed.take(5))

'''we omit the first line, just a header with no data'''
header=parsed.first()
noHeader=parsed.filter(lambda x:x!=header).cache()
print(noHeader.take(5))