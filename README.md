# textprocessing-pyspark

## By Spyridon Kaperonis

## Text used 
  [Letters to a Friend](https://www.gutenberg.org/files/56130/56130-h/56130-h.htm)

## Tools Used
1. Language: Python
2. Tools: Databdircks, PySpark, Matplotlib, Seaborn, Urllib

# Getting the text

1. First, we need to request for the text from the link of its plain text. 

```python
import urllib.request
urllib.request.urlretrieve("https://www.gutenberg.org/files/56130/56130-h/56130-h.htm")

```

1. Second, store the data in a temporary file.

```
from pyspark.dbutils import DBUtils
dbutils.fs.mv("file:/tmp/tmp25vryq57","dbfs:/data/Letters.txt")
```

1. Then, we need to get the data from the temporary file

```
letterRDD = sc.textFile("dbfs:/data/Letters.txt")
```

1. We will flatmap the data to start the cleanning process. With this command below we will remove empty spaces. 

```
words_RDD = letterRDD.flatMap(lambda line: line.lower().strip().split(" "))
```

1. The next step is to map the data and remove any punctuation using re.

```
import re
letterPairsRDD = words_RDD.map(lambda word: re.sub(r'[^a-zA-Z]','',word))
```

1. Now, we need to remove stopwords. 

```
from pyspark.ml.feature import StopWordsRemover
remover = StopWordsRemover()
stopwords = remover.getStopWords()
print(stopwords)
justWordsRDD = letterPairsRDD.filter(lambda word: word not in stopwords)
```

1. This command below will map words to key value pairs

```
kpairs = justWordsRDD.map(lambda word: (word,1))
```

1. Next we will reduce by key

```
letterWordCountRDD = kpairs.reduceByKey(lambda acc, value: acc + value)
```
