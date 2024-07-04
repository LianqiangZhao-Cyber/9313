from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql.functions import udf, col
from pyspark.sql.types import ArrayType, StringType, StructType, StructField
from pyspark.sql.functions import explode, array, lit, size, struct

import re
import sys

class Project2:           
    def run(self, inputPath, outputPath, stopwords, k):
        spark = SparkSession.builder.master("local").appName("project2_df").getOrCreate()
        
        stopwords_df = spark.read.text(stopwords)
        stopwords_list = [row.value for row in stopwords_df.collect()]
        
        # Broadcast stopwords list
        broadcast_stopwords = spark.sparkContext.broadcast(stopwords_list)
        
        headlines_df = spark.read.text(inputPath).toDF("line")
        
        def preprocessings(line):
            parts = line.lower().split(",",1)
            if len(parts) <2:
                return ("",[])
            category, headline = parts
            words = re.split("[\\s*$&#/\"'\\,.:;?!\\[\\](){}<>~\\-_]+", headline)
            words = [word for word in words if word.isalpha() and word not in broadcast_stopwords.value]
            return (category, words)
        
        """+----------------+----------------------------+
           | line           | preprocessed               |
           +----------------+----------------------------+
           | Hello world    | {example_category, [Hello, world]} |
           | Spark is great | {example_category, [Spark, is, great]} |
           +----------------+----------------------------+"""
        preprocess_udf = udf(lambda line: preprocessings(line), 
                             # Type or returned value
                             StructType([StructField("category", StringType(), True), StructField("words", ArrayType(StringType()), True)]))


        # selected preprocessed.* means only choose column being preprocessed. ignore "line"
        
        """
        +----------+--------------------+                                               
        |  category|               words|
        +----------+--------------------+
        |technology|[Google, Android,...|
        """
        preprocessed_df = headlines_df.withColumn("preprocessed", preprocess_udf(col("line"))).select("preprocessed.*")
        
        
        
        """
        +----------+--------------------+                                               
        |  category|               words|
        +----------+--------------------+
        |technology|[google, android,...|
        """
        valid_df = preprocessed_df.filter("size(words) >= 3")
        invalid_df = preprocessed_df.filter("size(words) < 3")
        
        """
        +----------+-------------+                                                      
        |  category|invalid_count|
        +----------+-------------+
        |  medicine|            1|
        |technology|            2|
        |  business|            2|
        +----------+-------------+
        """
        invalid_line_count_df = invalid_df.groupBy("category").count().withColumnRenamed("count", "invalid_count")
        
        """
        +----------+-----------+                                                        
        |category  |valid_count|
        +----------+-----------+
        |medicine  |5          |
        |technology|6          |
        |business  |5          |
        +----------+-----------+
        """
        valid_line_count_df = valid_df.groupBy("category").count().withColumnRenamed("count", "valid_count")
        
        # generate all possible triads
        def get_triads(words):
            triads = []
            # traids = [(word1, word2, word3), (word1, word2, word4),...]
            for i in range(len(words)):
                for j in range(i + 1, len(words)):
                    for k in range(j + 1, len(words)):
                        triads.append(tuple(sorted([words[i], words[j], words[k]])))
            return triads
        
        # traids = [(word1, word2, word3), (word1, word2, word4),...]
        triad_schema = ArrayType(StructType([
                        StructField("word1", StringType(), True),
                        StructField("word2", StringType(), True),
                        StructField("word3", StringType(), True)
                    ]))
        get_triads_udf = udf(lambda words: get_triads(words),triad_schema)
        
        """
        +----------+----------------------------------+                                 
        |category  |triad                             |
        +----------+----------------------------------+
        |technology|{android, google, sdk}            |
        |technology|{android, google, smartwatches}   |
        """
        triads_df = valid_df.withColumn("triads", explode(get_triads_udf(col("words")))).select("category", col("triads").alias("triad"))
        # countsDF = wordsDF.groupBy("word").agg(count("*").alias("count"))
        
        # count how many repeated triads
        countsDf = triads_df.groupBy("category", "triad").count().withColumnRenamed("count", "triad_count")
        
        joined_df = countsDf.join(valid_line_count_df, on="category")
        resultDf = joined_df.withColumn("count", col("triad_count") / col("valid_count")).drop("triad_count", "valid_count")


        # use window to do the ranking based on count
        windowSpec = Window.partitionBy("category").orderBy(col("count").desc())

        # row_number() function
        rankedDf = resultDf.withColumn("row_number", row_number().over(windowSpec))
 
        #filter the top k triads for each category
        """
        +----------+-----------------------------------+------------------+             
        |category  |triad                              |count             |
        +----------+-----------------------------------+------------------+
        |business  |{asia, discouraging, stocks}       |0.6               |
        |business  |{asia, discouraging, news}         |0.6               |
        |medicine  |{blood, contamination, hospital}   |0.6               |
        |medicine  |{contamination, hospital, patients}|0.4               |
        |technology|{cutting, jobs, microsoft}         |0.5               |
        |technology|{jobs, path, signals}              |0.3333333333333333|
        +----------+-----------------------------------+------------------+
        """
        topkDf = rankedDf.filter(col("row_number") <= k).drop("row_number")
        
        # concatnate triad and count into a string
        formattedDf = topkDf.withColumn("triad", concat_ws(",", col("triad.word1"), col("triad.word2"),col("triad.word3")))\
                            .withColumn("formatted", concat_ws(":", col("triad"), col("count"))).select("category", "formatted")
                            
                            
        """
        +-------------+---------------+                                                 
        |category     |result         |
        +-------------+---------------+
        |medicine     |invalid_line:31|
        |technology   |invalid_line:39|
        |sport        |invalid_line:4 |
        |entertainment|invalid_line:20|
        |business     |invalid_line:31|
        +-------------+---------------+

        """
        # for later insertion
        final_invalid_line = invalid_line_count_df.withColumn("result",concat(lit("invalid_line:"),col("invalid_count"))).select("category","result")


        # 将result列重命名为formatted，并添加一个顺序列
        df1 = final_invalid_line.withColumnRenamed("result", "formatted").withColumn("order", lit(0))
        df2 = formattedDf.withColumn("order", lit(1))

        # merge two DataFrames based on the same column
        merged_df = df1.unionByName(df2).orderBy("category", "order").show()

        # 删除order列
        merged_df = merged_df.drop("order")

        # 显示结果
        merged_df.write.option("delimiter", "\t").format("csv").save(outputPath)
        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Wrong arguments")
        sys.exit(-1)
    Project2().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])

