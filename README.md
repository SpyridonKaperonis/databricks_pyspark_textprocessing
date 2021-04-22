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

2. Second, store the data in a temporary file.

```
from pyspark.dbutils import DBUtils
dbutils.fs.mv("file:/tmp/tmp25vryq57","dbfs:/data/Letters.txt")
```

3. Then, we need to get the data from the temporary file

```
letterRDD = sc.textFile("dbfs:/data/Letters.txt")
```

4. We will flatmap the data to start the cleanning process. With this command below we will remove empty spaces. 

```
words_RDD = letterRDD.flatMap(lambda line: line.lower().strip().split(" "))
```

5. The next step is to map the data and remove any punctuation using re.

```
import re
letterPairsRDD = words_RDD.map(lambda word: re.sub(r'[^a-zA-Z]','',word))
```

6. Now, we need to remove stopwords. 

```
from pyspark.ml.feature import StopWordsRemover
remover = StopWordsRemover()
stopwords = remover.getStopWords()
print(stopwords)
justWordsRDD = letterPairsRDD.filter(lambda word: word not in stopwords)
```

7. This command below will map words to key value pairs

```
kpairs = justWordsRDD.map(lambda word: (word,1))
```

8. Next we will reduce by key

```
letterWordCountRDD = kpairs.reduceByKey(lambda acc, value: acc + value)
```

9. Last step of cleanning is to collect

```
results = letterWordCountRDD.collect()
```

## Charting

1. First, we need to import pandas and matplotlib

```
import matplotlib.pyplot as plt
import pandas as pd
```

2. Then, we will sort the results. Pick the most common, and get only the 10 first because there are a lot of data.

```
results.sort(key=lambda y:y[1])
wordCounter = Counter(results).most_common()
mostCommon = wordCounter[1:10]

```
3. Lastly, we will make a barplot

```
xlabel = 'Words'
ylabel = 'Count'
df = pd.DataFrame.from_records(mostCommon, columns=[xlabel, ylabel])

plt.figure(figsize=(10,3))
sns.barplot(xlabel, ylabel, data=df, palette="Greens_d").set_title("Letters to a Friend text count")
```

![Plot](https://github.com/SpyridonKaperonis/textprocessing-pyspark/blob/main/Capture.JPG)
