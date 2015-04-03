#!/usr/bin/env python
from __future__ import print_function
__author__ = 'rim'

import sys

for line in sys.stdin:
    if line[0] != "\"":
        citing, cited = line.rstrip().split(',')
        print(citing, cited, sep='\t')
        print(cited)