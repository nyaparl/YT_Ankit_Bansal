import os
import csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def  sparkSession():
    return SparkSession.builder.appName('first') \
        .master('local[*]').getOrCreate()

def  genearateInputFile(data,columns):
    if os.path.exists("input.csv"):
        os.remove("input.csv")

    with open("input.csv", 'w', newline='') as file:
        writer = csv.writer(file)

        writer.writerow(columns)

        for e in data:
            writer.writerow(list(e))