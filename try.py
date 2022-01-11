# from pyspark.ml.feature import HashingTF, IDF, Tokenizer
# from pyspark.context import SparkContext
from langdetect import detect
from langdetect.detector_factory import detect_langs
# from pyspark.sql.session import SparkSession
import networkx as nx
# spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
# # rdd = spark.sparkContext.parallelize(data)
# sentenceData = spark.createDataFrame([
#     (0.0, "😊 Hi I heard about Spark"),
#     (0.0, "I wish Java could use case classes"),
#     (1.0, "Logistic regression models are neat")
# ], ["label", "sentence"])
# tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
# wordsData = tokenizer.transform(sentenceData)
# wordsData.show()

# hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20)
# featurizedData = hashingTF.transform(wordsData)
# # alternatively, CountVectorizer can also be used to get term frequency vectors

# idf = IDF(inputCol="rawFeatures", outputCol="features")
# idfModel = idf.fit(featurizedData)
# rescaledData = idfModel.transform(featurizedData)

# rescaledData.select("label", "features").show()

# g = nx.Graph()
# g.add_edge("A", "B", weight = 1)
# print(g['A']['B']['weight'])
X = [[0,0,0], [3,2,2], [1,3,3], [0,0,1], [0,1,0]]
from sklearn.neighbors import kneighbors_graph
A = kneighbors_graph(X, 3, mode='connectivity', include_self=False)
print(A.toarray())


def filterChinese(msg):
    language = None
    try:
        language = detect(msg)
    except:
        language = "error"
        return True 
    print(detect_langs(msg))

    if language == 'zh-cn' or language == 'ja' or language == 'ko':
            return False
    return True

print(filterChinese("detect(没有测试iphone的机)"))
