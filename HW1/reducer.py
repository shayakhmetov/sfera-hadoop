#!/usr/bin/env python
__author__ = 'rim'
import sys
import math


def get_minimum(countries):
    return min(countries.values())


def get_median(countries):
    sorted_countries = sorted(countries.values())
    if len(sorted_countries) % 2 != 0:
        return sorted_countries[len(sorted_countries)/2]
    else:
        return (sorted_countries[len(sorted_countries)/2 - 1] + sorted_countries[len(sorted_countries)/2])/2.0


def get_max_value(countries):
    return max(countries.values())


def get_average(countries):
    return sum(countries.values()) / (1.0 * len(countries))


def get_dispersion(countries):
    average = get_average(countries)
    return math.sqrt(sum([pow(c - average, 2) for c in countries.values()]) / (1.0 * len(countries)))



def main():
    current_year = None
    years = {}
    for line in sys.stdin:
        year, country = line.rstrip().split('\t')
        year = int(year)
        if year == current_year:
            if country in years[year]:
                years[year][country] += 1
            else:
                years[year][country] = 1
        else:
            current_year = year
            years[year] = {country: 1}

    for year, countries in sorted(years.items()):
        sys.stdout.write('\t'.join([str(o) for o in [year, len(countries),
                                    get_minimum(countries),
                                    get_median(countries),
                                    get_max_value(countries),
                                    get_average(countries),
                                    get_dispersion(countries)]]) + '\n')

if __name__ == '__main__':
    main()