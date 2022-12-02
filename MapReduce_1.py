# Most frequent weather in each city 

from mrjob.job import MRJob
from mrjob.step import MRStep

class FrequentWeather(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper1, reducer=self.reducer1),
                MRStep(mapper = self.mapper2, reducer=self.reducer2)]

    def mapper1(self, _, row):
        row = row.split(',')
        city, weather = row[1], row[9]
        yield ((city, weather), 1)

    def reducer1(self, city_weather, count):
        yield (city_weather, sum(count))

    def mapper2(self, city_weather, count):
        city, weather  = city_weather
        yield (city, (weather, count))

    def reducer2(self, city, weather_count):
        yield (city, max(weather_count, key = lambda x : x[1]))

if __name__ == "__main__":
    FrequentWeather.run()