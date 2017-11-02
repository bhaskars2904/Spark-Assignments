from pyspark import SparkConf, SparkContext
from operator import add
import sys

## Constants

APP_NAME = "Twitter sentiment"

##OTHER FUNCTIONS/CLASSES

## Main functionality

def main(sc,filename):

	rdd = sc.textFile(filename) 
	rdd_tags = rdd.flatMap(lambda line: line.split(" ")).filter(lambda word: word.startswith('#')).filter(lambda y: len(y)>1).map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y).takeOrdered(100, key = lambda x: -x[1])#.sortBy(lambda x: x[1], ascending=False)
	sc.parallelize(rdd_tags).coalesce(1).saveAsTextFile('Results/sparkout.txt')
if __name__ == "__main__":
   # Configure Spark
   conf = SparkConf().setAppName(APP_NAME)
   conf = conf.setMaster('spark://zemoso-Lenovo-G580:7077')
   sc   = SparkContext(conf=conf)
   filename = sys.argv[1]
# Execute Main functionality
main(sc, filename)