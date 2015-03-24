#!/usr/bin/env python
__author__ = 'rim'
import sys


for i, line in enumerate(sys.stdin):
    if i != 0:
        line = line.rstrip().split(',')
        year = line[1]
        country = line[4]
        sys.stdout.write('\t'.join([year, country]) + '\n')
