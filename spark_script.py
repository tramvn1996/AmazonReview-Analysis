from pyspark.sql import SparkSession
from pyspark.sql.functions import exp

spark = SparkSession.builder.appName("amazon reviews").getOrCreate()
#merged = spark.read.json("file:///bigdata/data/amazon-reviews/merged.json").rdd
merged = spark.read.json("file:///home/tnguyen/BigData/project2/small.json").rdd



def cats(review):
    s = []
    if review['categories'] is not None:
        for cat in review['categories']:
            if cat is not None:
                for cat2 in cat:
                    s.append((cat2, 1))
    return s
count = merged.flatMap(cats)
#print(count.collect())

countrow=count.reduceByKey(lambda accum, new: accum+new)
#print(countrow.collect())
count_df = countrow.toDF(['key','count'])
def cats_review(review):
    s=[]
    if review['categories'] is not None:
        for cat in review['categories']:
            if cat is not None:
                for cat2 in cat:
                    s.append((cat2,review['overall']))
    return s
rank = merged.flatMap(cats_review)
#rank = merged.flatMap(lambda review: (cat, review['overall']) for cat in review['categories'])
print(rank.collect())
summ = rank.reduceByKey(lambda accum, new: accum+new)
summ_df = summ.toDF(['key','summ'])
#summ = rank.reduceByKey(lambda (accumsum, accumcount), (newsum, newcount): (accumsum+newsum,accumcount+accumcount+newcount))
avg = summ.join(countrow)
avg_df = summ_df.join(count_df, summ_df.key==count_df.key,'outer')
avg_df.show(n=2)
av = avg_df['summ']/avg_df['count']
avg_2 = avg_df.withColumn('total', av)
avg_2.toPandas().to_csv('/home/tnguyen/BigData/project2/result.csv')

avg_2.show()
