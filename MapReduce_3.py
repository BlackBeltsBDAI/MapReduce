
from mrjob.job import MRJob
from mrjob.step import MRStep

class FrequentTemp(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper1, reducer=self.reducer1),
                MRStep(mapper = self.mapper2, reducer=self.reducer2)]

    def mapper1(self, _, row):
        row = row.split(',')
        city, temp = row[1], row[10]
        yield ((city,temp),1)

    def reducer1(self, city_temp, values):
        yield (city_temp,sum(values))

    def mapper2(self, city_temp, values):
        city, temp  = city_temp
        yield (city, (temp, values))

    def reducer2(self, city, temp_values):
        yield (city, max(temp_values, key = lambda x : x[1]))


if __name__ == "__main__":
    FrequentTemp.run()
