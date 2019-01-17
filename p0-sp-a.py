from operator import *
from pyspark import *

conf = (SparkConf().setMaster("local").setAppName("SubProjectA").set("spark.executor.memory","1g"))
sc = SparkContext(conf = conf)

def remove_blank(x):
	if(x != ""):
		return(x)
	
def has_more_than_two_occurence(x):
	if(x[1]>2):
		return(x)

file=sc.textFile("C:\\Users\\susha\\OneDrive\\Desktop\\DSPTests\\p0\\data\\4300-0.txt")
file= file+sc.textFile("C:\\Users\\susha\\OneDrive\\Desktop\\DSPTests\\p0\\data\\pg36.txt")
file= file+sc.textFile("C:\\Users\\susha\\OneDrive\\Desktop\\DSPTests\\p0\\data\\pg514.txt")
file= file+sc.textFile("C:\\Users\\susha\\OneDrive\\Desktop\\DSPTests\\p0\\data\\pg1497.txt")
file= file+sc.textFile("C:\\Users\\susha\\OneDrive\\Desktop\\DSPTests\\p0\\data\\pg3207.txt")
file= file+sc.textFile("C:\\Users\\susha\\OneDrive\\Desktop\\DSPTests\\p0\\data\\pg6130.txt")
file= file+sc.textFile("C:\\Users\\susha\\OneDrive\\Desktop\\DSPTests\\p0\\data\\pg19033.txt")
file= file+sc.textFile("C:\\Users\\susha\\OneDrive\\Desktop\\DSPTests\\p0\\data\\pg42671.txt")

wordCount= file.flatMap(lambda x:x.split(' ')).filter(lambda x:remove_blank(x)).map(lambda x:x.lower()).map(lambda x:(x,1)).reduceByKey(add).filter(lambda x:has_more_than_two_occurence(x))

topFourtyWords=wordCount.map(lambda x:(x[1],x[0])).sortByKey(False).map(lambda x:(x[1],x[0])).take(40)
 
sc.parallelize(topFourtyWords).saveAsTextFile("C:\\Users\\susha\\OneDrive\\Desktop\\DSPTests\\tests\\sp1.json")