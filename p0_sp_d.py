#!/usr/bin/python
from operator import *
from pyspark import *
from string import punctuation
import json
import os
import re
import math
import sys

conf = (SparkConf().setMaster("local").setAppName("SubProjectB"))
sc = SparkContext(conf = conf)

def remove_blank(x):
	"""creating a function to remove the empty words"""
	if(x != ""):
		return(x)
			
def has_more_than_two_occurence(x):
	"""creating a function for finding words with more than 2 occurences"""
	if(x[1]>1):
		return(x)
			
def length_more_than_one(x):
	"""creating a function for checking the length of word is greater than 1"""
	if(len(x[0])>1):
		return(x)
		
def not_in_stop_words(stopWords,words):
	"""creating a function for finding words not in stop words"""
	if not(words in stopWords.value):
		return(words)
		
def remove_Punctuations(words):
	punctuations = (".", ",", ";", ":", "'", "?", "!")
	#while(words.startswith(punctuations)) or (words.endswith(punctuations)):
	if words.startswith(punctuations):		
		words = words[1:]
	if words.endswith(punctuations): 
		words = words[:-1]
		#else: 
		#	words=words
	return words
		
def interchanging_key_and_value(x):
	"""creating a function for interchanging key and value"""
	return (x[1],x[0])
	
def case_insensitive(x):	
	"""creating a function for interchanging key and value"""
	#return  str(x.encode('utf-8')).lower()
	return  x.lower()
		
def create_a_key_value_pair(x):	
	"""creating a function for creating a key value pair"""
	return (x,1)
	
def write_to_JSON_file(path, fileName, data):
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

NumberOfFiles=fileWithoutStopWord.count()

#wordCount with case-insensitive words and filetering words with more than 2 occurences in the text file and without stop words
#wordCount= fileWithoutStopWord.map(lambda x:case_insensitive(x[1])).flatMap(lambda x:x.split()).filter(lambda x:not_in_stop_words(stopWordsBroadcast,x)).map(lambda x:create_a_key_value_pair(x)).reduceByKey(add)
#wordCountFinal=wordCount.filter(lambda x:has_more_than_two_occurence(x)).filter(lambda x:length_more_than_one(x)).map(lambda x:(remove_Punctuations(x[0]),x[1]))

wordCountDocumentList=[]
wordOnlyArray=[]   
finalDocumentList=[]

"""Creating a list of list containing tuples of every word and the number of times it appears in the document "wordCountDocumentList".
   Creating a list conating all the words from all teh documents "wordOnlyArray"."""
for documents in fileWithoutStopWord.collect():
	documentsRDD= sc.textFile(documents[0])
	wordCountDocument=sc.parallelize(documents).map(lambda x:case_insensitive(x)).flatMap(lambda x:x.split()).filter(lambda x:not_in_stop_words(stopWordsBroadcast,x)).map(lambda x:create_a_key_value_pair(x)).filter(lambda x:len(x[0])>1).map(lambda x:(remove_Punctuations(x[0]),x[1])).reduceByKey(add)
	wordCountDocumentList.append(wordCountDocument.collect())
	wordOnlyArray.extend(wordCountDocument.collect())
	
data=sc.parallelize(wordOnlyArray)

count = data.combineByKey(lambda value: (value, 1),lambda x, value: (x[0] + value, x[1] + 1),lambda x, y: (x[0] + y[0], x[1] + y[1]))

"""Calculate the IDF and craete a dict with the words and teh calculated IDF values."""
dictWithIDF=count.map(lambda x: (x[0],math.log(NumberOfFiles/x[1][1]))).collectAsMap()

"""Multiply the calculated IDF with the wordCount of each words to get the TF-IDF value for all the words in a document.
   Sort the list and take only the top 5 data from the list"""
for doc in wordCountDocumentList:
	document=sc.parallelize(doc)
	listWithTFIDF=document.map(lambda x:(x[0],x[1]*dictWithIDF[x[0]])).map(lambda x:interchanging_key_and_value(x)).sortByKey(False).map(lambda x:interchanging_key_and_value(x)).take(5)
	for list in listWithTFIDF:
		finalDocumentList.append(list)
		
		
"""Python code to calculate the same TF-IDF without using RDD #WORKS"""	
#for i in wordCountDocumentList:
#	for j in i:
#		wordOnlyArray.extend(j[0])

#for i in wordCountDocumentList:
#	eachDocumentList=[]
#	for j in i:
#		count=wordOnlyArray.count(j[0])
	
#		newWord=(j[0],j[1]*math.log(NumberOfFiles/count))
#		eachDocumentList.append(newWord)
#	eachDocumentListRDD=sc.parallelize(eachDocumentList)
#	eachDocumenFinalRDD=eachDocumentListRDD.map(lambda x:interchanging_key_and_value(x)).sortByKey(False).map(lambda x:interchanging_key_and_value(x))
#	eachDocumenFinalFive=eachDocumenFinalRDD.take(5)
#	for a in eachDocumenFinalFive:
#		finalDocumentList.append(a)
		 
topFourtyWordsRDD=sc.parallelize(finalDocumentList)
topFourtyWordsRDDDict = topFourtyWordsRDD.collectAsMap()

"""saving the output as a JSON """
write_to_JSON_file(sys.argv[2],sys.argv[3],topFourtyWordsRDDDict)
