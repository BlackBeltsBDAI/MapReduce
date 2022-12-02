
# Most frequent weather in each month

from mrjob.job import MRJob
from mrjob.step import MRStep

class FrequentWeatherMonth(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper1, reducer=self.reducer1),
                MRStep(mapper = self.mapper2, reducer=self.reducer2)]

    def mapper1(self, _, row):
        row = row.split(',')
        month, weather = row[5], row[9]
        yield ((month, weather), 1)

    def reducer1(self, month_weather, count):
        yield (month_weather, sum(count))

    def mapper2(self, month_weather, count):
        month, weather  = month_weather
        yield (month, (weather, count))

    def reducer2(self, month, weather_count):
        yield (month, max(weather_count, key = lambda x : x[1]))

if __name__ == "__main__":
    FrequentWeatherMonth.run()

