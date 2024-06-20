"""

For each city, calculate the monthly average temperature in each year (monthly average for short).

Input: 

Australia/South Pacific	Australia	Brisbane	1	5	1998	75.7
Australia/South Pacific	Australia	Melbourne	1	5	1999	79.3
Australia/South Pacific	Australia	Melbourne	1	1	1998	75.6
Australia/South Pacific	Australia	Brisbane	1	4	1998	77.1
Australia/South Pacific	Australia	Brisbane	1	7	1999	76
"""
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env

class proj1(MRJob):   
    def mapper(self, _, line):
        data = line.strip().split('\t')
        if len(data) == 7:
            city = data[2]
            year = data[5]
            month = data[3]
            temp_fahrenheit = float(data[6])
            yield (city, year,month), temp_fahrenheit
            
    def reducer(self, key, values):
        total_temp = 0
        count = 0
        for temp in values:
            total_temp += temp
            count += 1
        avg_temp = total_temp / count
        yield key, avg_temp
        
            
    # Fill in your code here

if __name__ == '__main__':
    proj1.run()
