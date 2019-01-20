from operator import *
from pyspark import *
from string import punctuation
import json

conf = (SparkConf().setMaster("local").setAppName("SubProjectB"))
sc = SparkContext(conf = conf)

#creating a function to remove the empty words
def remove_blank(x):
	if(x != ""):
		return(x)
		
#creating a function for finding words with more than 2 occurences	
def has_more_than_two_occurence(x):
	if(x[1]>1):
		return(x)
		
#creating a function for checking the length of word is greater than 1		
def length_more_than_one(x):
	if(len(x[0])>1):
		return(x)
		
#creating a function for finding words not in stop words	
def not_in_stop_words(stopWords,words):
	if not(words in stopWords.value):
		return(words)
		
#creating a function for removing the punctuations
def remove_Punctuations(words):
	if(len(words)>1):
		return(words.strip(punctuation))
		
#creating a function for interchanging key and value
def interchanging_key_and_value(x):
	return(x[1],x[0])
	
#creating a function for interchanging key and value	
def case_insensitive(x):	
	return x.lower()

#creating a function for writing into an json file	
def write_to_JSON_file(path, fileName, data):
    filePathNameWExt = path + '//' + fileName + '.json'
    with open(filePathNameWExt, 'w') as fp:
        json.dump(data, fp) 

#Merging all the files into a single text file
file= sc.wholeTextFiles("C:\\Users\\susha\\OneDrive\\Desktop\\DSPTests\\p0\\data")
fileWithoutStopWord=file.filter(lambda x:(not("stopwords.txt" in x[0]))) 

#declaring the stop words as a broadcast file
stopWordsFile=file.filter(lambda x:("stopwords.txt" in x[0])) 
stopWords=stopWordsFile.flatMap(lambda x:x[1].split('\n'))
stopWordsBroadcast=sc.broadcast(stopWords.collect())

#wordCount with case-insensitive words and filetering words with more than 2 occurences in the text file and without stop words
wordCount= fileWithoutStopWord.map(lambda x:case_insensitive(x[1])).flatMap(lambda x:x.split()).filter(lambda x:not_in_stop_words(stopWordsBroadcast,x)).map(lambda x:(x,1)).reduceByKey(add)
wordCountFinal=wordCount.filter(lambda x:has_more_than_two_occurence(x)).filter(lambda x:length_more_than_one(x)).map(lambda x:(remove_Punctuations(x[0]),x[1]))

#taking only the top 40 most frequently occuring words
topFourtyWords=wordCountFinal.map(lambda x:interchanging_key_and_value(x)).sortByKey(False).map(lambda x:interchanging_key_and_value(x)).take(40)

topFourtyWordsRDD=sc.parallelize(topFourtyWords)
topFourtyWordsRDDDict = topFourtyWordsRDD.collectAsMap()

#saving the output as a JSON 
write_to_JSON_file("C:\\Users\\susha\\OneDrive\\Desktop\\DSPTests\\tests","sp3",topFourtyWordsRDDDict)