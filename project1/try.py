from mrjob.job import MRJob
from mrjob.step import MRStep
import sys
from mrjob.compat import jobconf_from_env

"""
Australia/South Pacific	Australia	Brisbane	1	5	1998	75.7
Australia/South Pacific	Australia	Melbourne	1	5	1999	79.3
Australia/South Pacific	Australia	Melbourne	1	1	1998	75.6
Australia/South Pacific	Australia	Brisbane	1	4	1998	77.1
Australia/South Pacific	Australia	Brisbane	1	7	1999	76

"""

    
class proj1(MRJob):   
    JOBCONF = {
        'stream.num.map.output.key.fields': 2,
        'mapreduce.map.output.key.field.separator': '#',
        'mapreduce.partition.keypartitioner.options': '-k1,1',
        'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
        'mapreduce.partition.keycomparator.options': '-k1,1 -k2,2nr'
    }


    def mapper(self, _, line):
        data = line.strip().split('\t')
        if len(data) == 7:
            city = data[2]
            year = data[5]
            temp_fahrenheit = float(data[6])
            # Convert temperature to Celsius
            temp_celsius = (temp_fahrenheit - 32) * (5 / 9)
            yield city+"#"+year, temp_celsius
            yield city+"#9999", temp_celsius
            
    def combiner(self, key, values):
        count = 0
        total = 0
        for value in values:
            total += value
            count += 1
        yield key, (total, count)
        
    def reducer_init(self):
        self.tau = float(jobconf_from_env('myjob.settings.tau'))
        self.avg_city = 0
        

    def reducer(self, key, values):
        total = 0
        count = 0
        for value_sum, value_count in values:
            total += value_sum
            count += value_count
        avg = total / count
        city, year = key.split("#",1)
        if year == "9999":
            self.avg_city = avg
        else:
            avg_year_city = avg
            diff = avg_year_city - self.avg_city
            if diff > self.tau:
                yield city, (year+","+str(diff))
        


if __name__ == '__main__':
    proj1.run()
