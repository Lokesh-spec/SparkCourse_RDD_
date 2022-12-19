## Importing the libraries

import collections
from pyspark import SparkConf, SparkContext
from pprint import pprint


## Configurations
conf = SparkConf().setMaster('local').setAppName('RatingsHistorgram')
sc = SparkContext(conf=conf)


## Reading the Text file (Also called creating RDD)
lines = sc.textFile('ml-100k/u.data')

## user id | item id | rating | timestamp
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

pprint(result)


## Sort the result by items
sortedResults = collections.OrderedDict(sorted(result.items()))


for key, value in sortedResults.items():
    print("%s %i" % (key, value))