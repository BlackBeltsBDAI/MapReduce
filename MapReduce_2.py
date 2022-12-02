# Most frequent wind speed in each city 

from mrjob.job import MRJob
from mrjob.step import MRStep

class FrequentWind(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper1, reducer=self.reducer1),
                MRStep(mapper = self.mapper2, reducer=self.reducer2)]

    def mapper1(self, _, row):
        row = row.split(',')
        city, wind = row[1], row[11]
        yield ((city, wind), 1)

    def reducer1(self, city_wind, count):
        yield (city_wind, sum(count))

    def mapper2(self, city_wind, count):
        city, wind  = city_wind
        yield (city, (wind, count))

    def reducer2(self, city, wind_count):
        yield (city, max(wind_count, key = lambda x : x[1]))

if __name__ == "__main__":
    FrequentWind.run()