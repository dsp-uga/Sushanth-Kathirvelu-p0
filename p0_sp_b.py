#!/usr/bin/python
from operator import *
from pyspark import *
import json
import sys

conf = (SparkConf().setMaster("local").setAppName("SubProjectB"))
sc = SparkContext(conf = conf)

def removeBlank(x):
	"""creating a function to remove the empty words"""
	if(x != ""):
		return(x)
		
def not_in_stop_words(stopWords,words):
	"""creating a function for finding words not in stop words"""
	if not(words in stopWords.value):
		return(words)
		
def case_insensitive(x):	
	"""creating a function for interchanging key and value"""
	return  x.lower()
		
def hasMoreThanTwoOccurence(x):
	"""creating a function for finding words with more than 2 occurences"""	
	if(x[1]>2):
		return(x)
		
def notInStopWords(stopWords,words):
	"""creating a function for finding words not in stop words"""	
	if not(words in stopWords.value):
		return(words)
		
def create_a_key_value_pair(x):	
	"""creating a function for creating a key value pair"""
	return (x,1)

def writeToJSONFile(path, fileName, data):
	#"""creating a function for writing into an json file"""
    filePathNameWExt = path + '//' + fileName + '.json'
    with open(filePathNameWExt, 'w') as fp:
        json.dump(data, fp) 

"""Merging all the files into a single text file"""
file= sc.wholeTextFiles(sys.argv[1])
fileWithoutStopWord=file.filter(lambda x:(not("stopwords.txt" in x[0]))) 

"""declaring the stop words as a broadcast file"""
stopWordsFile=file.filter(lambda x:("stopwords.txt" in x[0])) 
stopWords=stopWordsFile.flatMap(lambda x:x[1].split('\n'))
stopWordsBroadcast=sc.broadcast(stopWords.collect())

"""wordCount with case-insensitive words and filetering words with more than 2 occurences in the text file and without stop words"""
wordCount= fileWithoutStopWord.map(lambda x:case_insensitive(x[1])).flatMap(lambda x:x.split()).filter(lambda x:not_in_stop_words(stopWordsBroadcast,x)).map(lambda x:create_a_key_value_pair(x)).reduceByKey(add)
#wordCount= fileWithoutStopWord.flatMap(lambda x:x[1].split(' ')).filter(lambda x:removeBlank(x)).map(lambda x:x.lower()).filter(lambda x:notInStopWords(stopWordsBroadcast,x)).map(lambda x:(x,1)).reduceByKey(add).filter(lambda x:hasMoreThanTwoOccurence(x))
"""taking only the top 40 most frequently occuring words"""
topFourtyWords=wordCount.map(lambda x:(x[1],x[0])).sortByKey(False).map(lambda x:(x[1],x[0])).take(40)

topFourtyWordsRDD=sc.parallelize(topFourtyWords)
topFourtyWordsRDDDict = topFourtyWordsRDD.collectAsMap()

"""saving the output as a JSON """
writeToJSONFile(sys.argv[2],sys.argv[3],topFourtyWordsRDDDict)