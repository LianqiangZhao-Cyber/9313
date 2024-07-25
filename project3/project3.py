import sys
from pyspark import SparkConf, SparkContext, StorageLevel
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
        transaction_grouped = lines_parsed.groupByKey().persist(StorageLevel.MEMORY_AND_DISK)
        
        # (2, ({'C', 'DD', 'A'}, datetime.datetime(2010, 1, 13, 0, 26)))
        # (4, ({'V', 'DB', 'A'}, datetime.datetime(2010, 1, 13, 0, 0)))
        transaction_set = transaction_grouped.map(self.create_traction_set).sortByKey()
        """
        ((2, {'C', 'A', 'DD'}), (3, {'DB', 'V', 'A'}))
        ((2, {'C', 'A', 'DD'}), (4, {'DB', 'V', 'A'}))
        ((3, {'DB', 'V', 'A'}), (4, {'DB', 'V', 'A'}))
        """
        
        def not_same_year_month(pair):
            date1 = pair[0][1][1]
            date2 = pair[1][1][1]
            return date1.year != date2.year or date1.month != date2.month


        def common_prefix(pair):
            tranction1 = pair[0][1][0]
            tranction2 = pair[1][1][0]
            
            prefix_length = len(tranction1) - math.ceil(len(tranction1) * threshold.value) + 1
            prefix1 = set(tranction1[:prefix_length])
            prefix2 = set(tranction2[:prefix_length])
            
            return len(prefix1&prefix2) != 0
            
            
        # 使用mapvalues将set转化为list，方便比较prefix
        transaction_pairs = list(combinations(transaction_set.mapValues(lambda x: (sorted(x[0]), x[1])).collect(), 2))
        # 首先查看日期，然后再prefix filtering，最后计算相似度，再filter
        filtered_pairs = sc.parallelize(transaction_pairs)
        
        # 首先查看日期
        # ((1, ({'B', 'C', 'A'}, datetime.datetime(2010, 1, 12, 0, 0))), (3, ({'B', 'C', 'A', 'DD'}, datetime.datetime(2011, 1, 14, 0, 0))))
        # ((1, ({'B', 'C', 'A'}, datetime.datetime(2010, 1, 12, 0, 0))), (4, ({'B', 'C', 'A', 'DD'}, datetime.datetime(2011, 1, 14, 0, 0))))
        filtered_pairs_date = filtered_pairs.filter(not_same_year_month)
        
        
        # 然后判定prefix 是否有common elements
        # ((2, (['A', 'C', 'DD'], datetime.datetime(2010, 1, 13, 0, 0))), (3, (['A', 'B', 'C', 'DD'], datetime.datetime(2011, 1, 14, 0, 0))))
        
        # ((1, ['A', 'B', 'C']), (3, ['A', 'B', 'C', 'DD']))
        filtered_pairs_prefix = filtered_pairs_date.filter(common_prefix).map(lambda x:((x[0][0],x[0][1][0]),(x[1][0],x[1][1][0])))
        
        # 计算similarity
        filtered_pairs_similar = filtered_pairs_prefix.map(lambda x: ((x[0][0],x[1][0]), self.jaccard_similarity(set(x[0][1]), set(x[1][1]))))
        
        
        # filter
        filtered_pairs_similar.filter(lambda x: x[1] >= threshold.value).map(lambda x: f"({x[0][0]},{x[0][1]}):{x[1]}").sortBy(lambda x: x[0]).saveAsTextFile(outputpath)
        """filtered_pairs_similar \
        .filter(lambda x: x[1] >= threshold.value) \
        .map(lambda x: (x[0], x[1])) \
        .sortByKey() \
        .map(lambda x: f"({x[0][0]},{x[0][1]}):{x[1]}") \
        .saveAsTextFile(outputpath)"""
            
        
        
        
        
        
                                    
        
        
        # prefix filtering
        
        
        """# 存储最终结果
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
        sorted_result_rdd.map(lambda x: f"({x[0]},{x[1]}):{x[2]}").saveAsTextFile(outputpath)"""
        
        time.sleep(1000)  # 保持 SparkContext 活动状态，以便访问 Spark UI

        sc.stop()
        
if __name__ == '__main__':
    project3().run(sys.argv[1], sys.argv[2], sys.argv[3])
    

