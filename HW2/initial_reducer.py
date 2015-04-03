#!/usr/bin/env python
from __future__ import print_function
__author__ = 'rim'

import sys

d = 0.85

current_citing = None
current_citied = set()
for line in sys.stdin:
    line = line.rstrip().split('\t')
    if len(line) == 1:
        citing = line[0]
    elif len(line) == 2:
        citing, cited = line
    if citing == current_citing:
        if len(line) == 2:
            current_citied.add(cited)
    else:
        if current_citing and len(current_citied) != 0:
            print(current_citing, 1-d, sep='\t', end='\t')
            print(*current_citied, sep=',')
        elif current_citing:
            print(current_citing, 1-d, sep='\t')

        current_citing = citing
        if len(line) == 2:
            current_citied = set([cited])
        else:
            current_citied = set()

if current_citing and current_citied:
    print(current_citing, 1-d, sep='\t', end='\t')
    print(*current_citied, sep=',')
elif current_citing and not current_citied:
    print(current_citing, 1-d, sep='\t')