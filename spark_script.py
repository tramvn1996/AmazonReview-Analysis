from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

spark = SparkSession.builder.appName("amazon reviews").getOrCreate()
merged = spark.read.json("file:///bigdata/data/amazon-reviews/merged.json").rdd
#merged = spark.read.json("file:///home/tnguyen/BigData/project2/small.json").rdd



def cats(review):
    s = []
    if review['categories'] is not None:
        for cat in review['categories']:
            if cat is not None:
                for cat2 in cat:
                    s.append((cat2, 1))
    return s
#count = merged.flatMap(cats)
#print(count.collect())

#countrow=count.reduceByKey(lambda accum, new: accum+new)
#print(countrow.collect())
#count_df = countrow.toDF(['key','count'])
def cats_review(review):
    s=[]
    if review['categories'] is not None:
        for cat in review['categories']:
            if cat is not None:
                for cat2 in cat:
                    s.append((cat2,review['overall'],1))
    return s
rank = merged.flatMap(cats_review)
#rank = merged.flatMap(lambda review: (cat, review['overall']) for cat in review['categories'])
#summ = rank.reduceByKey(lambda accum, new: accum+new)
summ_df = rank.toDF(['key','summ','count'])
new_df = summ_df.groupby("key").sum()
#new_df = new_df.toDF(['key','summ','count'])
#summ = rank.reduceByKey(lambda (accumsum, accumcount), (newsum, newcount): (accumsum+newsum,accumcount+accumcount+newcount))
#avg = summ.join(countrow)
#avg_df = summ_df.join(count_df, summ_df.key==count_df.key,'outer')
#avg_df.show(n=2)
av = new_df['sum(summ)']/new_df['sum(count)']
avg_2 = new_df.withColumn('total', av)
avg_3 = avg_2.orderBy(desc("total"))
#avg_2new_df.show()
avg_3.toPandas().to_csv('bigresult.csv')

#print(summ.collect())
