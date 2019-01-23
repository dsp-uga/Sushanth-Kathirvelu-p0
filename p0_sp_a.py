#!/usr/bin/python
from operator import *
from pyspark import *
import json
from pyspark.sql import *
import sys

conf = (SparkConf().setMaster("local").setAppName("SubProjectA"))
sc = SparkContext(conf = conf)
  
def remove_blank(x):
	"""creating a function to remove the empty words"""
	if(x !=' ' ):
		return(x)
			
def has_more_than_two_occurence(x):
	"""creating a function for finding words with more than 2 occurences"""
	if(x[1]>1):
		return(x)
		
def interchanging_key_and_value(x):
	"""creating a function for interchanging key and value"""
	return(x[1],x[0])
 
def writeToJSONFile(path, fileName, data):
	#"""creating a function for writing into an json file"""
    filePathNameWExt = path + '//' + fileName + '.json'
    with open(filePathNameWExt, 'w') as fp:
        json.dump(data, fp)

"""Merging all the files into a single text file"""
#"C:\\Users\\susha\\OneDrive\\Desktop\\DSPTests\\p0\\data"
file= sc.wholeTextFiles(sys.argv[1])
fileWithoutStopWord=file.filter(lambda x:(not("stopwords.txt" in x[0]))) 	

print("count...",len(fileWithoutStopWord.collect()))


"""wordCount with case-insensitive words and filetering words with more than 2 occurences in the text file"""
wordCount= fileWithoutStopWord.map(lambda x:x[1].lower()).flatMap(lambda x:x.split()).map(lambda x:(x,1)).reduceByKey(add).filter(lambda x:has_more_than_two_occurence(x))

"""taking only the top 40 most frequently occuring words"""
topFourtyWords=wordCount.map(lambda x:interchanging_key_and_value(x)).sortByKey(False).map(lambda x:interchanging_key_and_value(x)).take(40)

	
topFourtyWordsRDD=sc.parallelize(topFourtyWords)
topFourtyWordsRDDDict = topFourtyWordsRDD.collectAsMap()

"""saving the output as a JSON"""
#C:\\Users\\susha\\OneDrive\\Desktop\\DSPTests
#sp1
writeToJSONFile(sys.argv[2],sys.argv[3],topFourtyWordsRDDDict)