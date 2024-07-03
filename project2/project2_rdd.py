from pyspark import SparkContext, SparkConf
import sys
import re

class Project2:   
        
    def run(self, inputPath, outputPath, stopwords, k):
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
            category, headline = line.lower().split(",")
            words = re.split(r"[\\s*$&#/\"',.:;?!\\[\\](){}<>~\\-_]+", headline)
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
        
        traids_count = headlines.map(preprocess)\
                                .flatMap(get_triad_frequency)\
                                .map(lambda traid: (traid, 1))\
                                .reduceByKey(lambda a, b: a+b)

            
            
            
        
        
        

        sc.stop()

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Wrong arguments")
        sys.exit(-1)
    Project2().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])

