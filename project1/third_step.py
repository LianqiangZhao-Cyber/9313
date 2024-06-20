from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env

class proj1(MRJob):   
    # in-mapper combiner
    def mapper_init(self):
        self.city_year_temps = {}
        self.city_temps = {}
        self.result= {}
    
    def mapper(self, _, line):
        data = line.strip().split('\t')
        if len(data) == 7:
            city = data[2]
            year = data[5]
            temp_fahrenheit = float(data[6])
            # Convert temperature to Celsius
            temp_celsius = (temp_fahrenheit - 32) * 5 / 9
            
            # Aggregate temperatures by city and year
            if (city, year) not in self.city_year_temps:
                self.city_year_temps[(city, year)] = []
            self.city_year_temps[(city, year)].append(temp_celsius)
            
            # Aggregate temperatures by city
            if city not in self.city_temps:
                self.city_temps[city] = []
            self.city_temps[city].append(temp_celsius)
    
    def mapper_final(self):

        # Emit city-year average temperatures
        for (city, year), temps in self.city_year_temps.items():
            avg_temp = sum(temps) / len(temps)
            self.city_year_temps[(city, year)] = avg_temp
            #yield (city, year), ('monthly', avg_temp)
        
        # Emit city overall average temperatures
        for city, temps in self.city_temps.items():
            avg_temp = sum(temps) / len(temps)
            self.city_temps[city] = avg_temp
            #yield city, ('overall', avg_temp)
            
        for key, monthly_temp in self.city_year_temps.items():
            city, year = key[0],key[1]
            avg_temp = self.city_temps[city]
            difference = monthly_temp - avg_temp
            if difference > 0.3:
                 self.result[(city, year)] = difference

        
        for key, value in self.result.items():

            yield key[0]+","+key[1]+","+str(value), None
    
    
    def reducer(self, key, values):
        yield key, None
    
    '''def reducer_init(self):
        # self.tap= int(jobconf_from_env('myjob.settings.topk'))
        self.city_overall_avg = {}
        self.city_year_avg = {}
    
    def reducer(self, key, values):
        if isinstance(key, list):
            city, year = key
            for value in values:
                if value[0] == 'monthly':
                    self.city_year_avg[city, year] = (value[1])
        else:
            city = key
            for value in values:
                if value[0] == 'overall':
                    self.city_overall_avg[city] = value[1]
                    
    
    def reducer_final(self):
        # Emit city overall average temperatures
        
        for key, monthly_temp in self.city_year_avg.items():
            city, year = key[0],key[1]
            avg_temp = self.city_overall_avg[city]
            difference = monthly_temp - avg_temp
            if difference > 0.5:
                yield city, (year, difference)'''
                
    SORT_VALUES = True
    def steps(self):
        JOBCONF = {
            'stream.num.map.output.key.fields':2,
            'mapreduce.map.output.key.field.separator':',',
            'mapreduce.partition.keypartitioner.options':'-k1,2',
            'mapreduce.job.output.key.comparator.class':'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
            'mapreduce.partition.keycomparator.options':'-k1,1 -k2,2nr' 
            
        }
        return [
            MRStep(jobconf=JOBCONF, mapper_init = self.mapper_init, mapper=self.mapper, mapper_final=self.mapper_final, reducer=self.reducer)
        ]



if __name__ == '__main__':
    proj1.run()

