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

'''we omit the first line, just a header with no data'''
header=parsed.first()
noHeader=parsed.filter(lambda x:x!=header).cache()


'''Now we calculate the average salary. First we have to filter all the valid fields
applying the filter "isSalary" to the not null values. Then the "$" is removed
and we calculate the average'''


def isSalary(line):
    pattern = '\$(\d+)\.(\d+)'
    return re.search(pattern,line)

avgSalary=noHeader.map(lambda x: x.salary).filter(lambda x: x is  not None).filter(isSalary).\
    map(lambda x: float(x.replace("$",""))).mean()

print("The average salary is: $"+str(avgSalary))

'''Now we look for the top-3 popular departments'''

topDepartemnt=noHeader.map(lambda x:(x.department,1)).reduceByKey(lambda a,b:a+b)

print("The top-3 popular departments are: "+str(topDepartemnt.take(3)))

"And now we get the top-3 people with the highest salary"


topSalaries=noHeader.map(lambda x: (x,x.salary)).\
    filter(lambda x: isSalary(str(x[1]))).map(lambda x:(x[0],float(x[1].replace("$",""))))
    #sortBy(lambda(k,v):-v)
print("The top-3 people with the highest salaries are: "+str(topSalaries.take(3)))


