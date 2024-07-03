from pyspark import SparkContext, SparkConf
import sys
import re
from collections import defaultdict

class Project2:   
        
    def run(self, inputPath, outputPath, stopwords, k):
        invalid_line = defaultdict(int)
        valid_line = defaultdict(int)
        # 1.Initialize SparkContext
        conf = SparkConf().setAppName("project2_rdd").setMaster("local")
        sc = SparkContext(conf=conf)
        
        # 2.load file
            # 2.0 load stopwords file
        with open(stopwords, 'r') as f:
            stopwords_list = [line.strip() for line in f]
        
            # 2.1 broadcast stopword_list 
        broadcast_stopwords = sc.broadcast(stopwords_list)
        
            # 2.2 load headline file
        headlines = sc.textFile(inputPath)

        
        # 3.Data preprocessing
        def preprocess(line):
            """
            1. Use the following split function to split the documents into terms:

                import re

                re.split("[\\s*$&#/\"'\\,.:;?!\\[\\](){}<>~\\-_]+", line)
            2. Ignore the letter case, i.e., consider all words as lowercase.

            3. Ignore terms that contain non-alphabetical characters, you can use isalpha().

            4. Ignore the stop words such as "to", "the", "in", etc."""
            category, headline = line.lower().split(",", 1)
            words = re.split("[\\s*$&#/\"'\\,.:;?!\\[\\](){}<>~\\-_]+", headline)
            words = [word for word in words if word.isalpha() and word not in broadcast_stopwords.value]
            return (category, words)
            
        # 4. compute traids frequency
        def get_triad_frequency(category_words):
            category, words = category_words
            triads = [(category, tuple(sorted([words[i], words[j], words[k]]))) 
                      for i in range(len(words)) 
                      for j in range(i+1, len(words)) 
                      for k in range(j+1, len(words))]
            return triads
        
        # count of each triad
        traids_count = headlines.map(preprocess).collect()
        for line in traids_count:
            if len(line[1])<3:
                invalid_line[line[0]] += 1
            else:
                valid_line[line[0]] +=1 
        traids_count = sc.parallelize(traids_count).flatMap(get_triad_frequency)\
                                .map(lambda traid: (traid, 1))\
                                .reduceByKey(lambda a, b: a+b)
        
        """# total count         
        
        category_counts = traids_count.map(lambda category: (category[0][0], category[1])) \
                                      .reduceByKey(lambda a, b: a + b)"""
        
        valid_line = sc.parallelize(list(valid_line.items()))
        # compute the relative frequency of each triad
        # ('business', ('comeback', 'penney', 'wall'), 0.0010277492291880781)]
        triad_relative_freq = traids_count.map(lambda x: (x[0][0], (x[0][1], x[1]))) \
                                          .join(valid_line) \
                                          .map(lambda x: (x[0], x[1][0][0], x[1][0][1] / x[1][1]))
        
        
        
        top_k_triads = triad_relative_freq.map(lambda x: (x[0], (x[1], x[2]))) \
                                          .groupByKey()\
                                          .flatMap(lambda x: [(x[0], triad[0], triad[1]) for triad in sorted(x[1], key=lambda y: -y[1])[:int(k)]]).sortBy(lambda x:x[0])

        
        
        invalid_line_broadcast = sc.broadcast(dict(invalid_line.items()))
        final_result = top_k_triads.map(lambda x: (x[0], f"{x[0]}\t{','.join(x[1])}:{x[2]}")) \
                                .groupByKey() \
                                .flatMap(lambda x: [f"{x[0]}\tinvalid line:{invalid_line_broadcast.value[x[0]]}"] + list(x[1]))\
                                .sortBy(lambda x: x.split('\t')[0])
        final_result.foreach(print)

        sc.stop()

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Wrong arguments")
        sys.exit(-1)
    Project2().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])

