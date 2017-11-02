from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window
import sys
import pyspark
import pyspark.sql.functions as func

## Constants

APP_NAME = "GDP Analysis"

##OTHER FUNCTIONS/CLASSES

## Main functionality

def main(spark,filename):
	df = spark.read.load(filename,format="csv", sep=",", inferSchema="true", header="true")
	print(df.columns)
	window = Window.partitionBy("Country Name").orderBy("Year")
	results = df.withColumn("growth", (df["Value"] - when( (lag(df["Value"],1).over(window)).isNull(), 0).otherwise(lag(df["Value"],1).over(window)))/(when( (lag(df["Value"],1).over(window)).isNull(), -1).otherwise(lag(df["Value"],1).over(window))))
	windowSpec = Window.partitionBy("Year").orderBy(results["growth"].desc())
	results.select("Country Name","Year",rank().over(windowSpec).alias('rank')).orderBy("Year").filter(col('rank') <= 1).drop("rank").coalesce(1).write.csv('Results/gdpout.csv')
	
if __name__ == "__main__":
	spark = SparkSession.builder.master('local[*]').appName(APP_NAME).getOrCreate()
	filename = sys.argv[1]
# Execute Main functionality
main(spark, filename)