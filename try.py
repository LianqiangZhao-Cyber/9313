import sys
from pyspark import SparkConf, SparkContext
from datetime import datetime
from itertools import combinations
import math
import time
# spark-submit project2_rdd.py file:///Users/homy/9313/9313/project2/sample.csv file:///Users/homy/9313/9313/project2/output3.txt  file:///Users/homy/9313/9313/project2/stopwords.txt 3


"""
1,A,12/1/2010  8:26:00 AM,2.5
1,B,12/1/2010  8:26:00 AM,2.3

"""

# parse lines
# -> (1, (A, 12/1/2010  8:26:00 AM))
# -> (1, (B, 12/1/2010  8:26:00 AM))

# -> (1, ({A,B}, 12/1/2010))
# -> (2, ({A,DD}, 12/1/2011))

# 两两similarity计算 先查看日期，如果年份或者月份不相同的话再计算similarity，similarity >= t 则输出

class project3:
    def parse_line(self,line):
        fields = line.split(',')
        transaction_id = int(fields[0])
        product_name = fields[1]
        transaction_date = datetime.strptime(fields[2].split(" ")[0], '%d/%m/%Y')
        
        return (transaction_id,(product_name, transaction_date))
        
    def create_traction_set(self,line):
        # transaction_id = 1
        # items = ('A', datetime.datetime(2010, 1, 12, 8, 26))
        transaction_id, items = line
        items_list = list(items)
        # {A, B, C}
        product_set = set(item[0] for item in items)
        
        date = items_list[0][1]
        return (transaction_id, (product_set, date))
    
    def jaccard_similarity(self, set1, set2):
        intersection = len(set1.intersection(set2))
        union = len(set1.union(set2))
        return intersection / union if union != 0 else 0.0
        
    def run(self, inputpath, outputpath, k):
        conf = SparkConf()
        sc = SparkContext(conf = conf)

        # broadcast threshold (as k will be access a lot and it wont change)
        threshold = sc.broadcast(float(k))
        lines  = sc.textFile(inputpath)
        
        # convert lines from  1,B,12/1/2010  8:26:00 AM,2.3 to (1, (A, 12/1/2010  8:26:00 AM))
        # (1, ('A', datetime.datetime(2010, 1, 12, 8, 26)))
        lines_parsed = lines.map(self.parse_line)
        # convert lines from (1, ('A', datetime.datetime(2010, 1, 12, 8, 26))) to (1, ({A, B}, datetime.datetime(2010, 1, 12)))
        # Intension: remove duplicates
        
        # (1,[('A', datetime.datetime(2010, 1, 12, 8, 26)), ('B', datetime.datetime(2010, 1, 12, 8, 26))])
        transactions_grouped = lines_parsed.groupByKey().cache()
        
        product_sets = transactions_grouped.map(self.create_traction_set)

        # 生成所有交易对
        product_sets_list = product_sets.collect()
        valid_pair = []
        
        for (trans1, data1), (trans2, data2) in combinations(product_sets_list, 2):
            set1,date1 = data1
            set2,date2 = data2
            if (date1.year != date2.year) or (date1.year == date2.year and date1.month != date2.month):
                similarity = self.jaccard_similarity(set1, set2)
                if similarity >= threshold.value:
                    valid_pair.append((trans1, trans2, similarity))
        
        sorted_result = sorted(valid_pair, key=lambda x: (x[0], x[1]))
        
        sorted_result_rdd = sc.parallelize(sorted_result)
        sorted_result_rdd.map(lambda x: f"({x[0]},{x[1]}):{x[2]}").saveAsTextFile(outputpath)
        
        time.sleep(1000)  # 保持 SparkContext 活动状态，以便访问 Spark UI

        sc.stop()
        
if __name__ == '__main__':
    project3().run(sys.argv[1], sys.argv[2], sys.argv[3])
    

