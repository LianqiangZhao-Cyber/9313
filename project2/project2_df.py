from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

class Project2:           
    def run(self, inputPath, outputPath, stopwords, k):
        spark = SparkSession.builder.master("local").appName("project2_df").getOrCreate()
        
        # Fill in your code here
       
        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Wrong arguments")
        sys.exit(-1)
    Project2().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])

