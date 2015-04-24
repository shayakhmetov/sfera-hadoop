#!/bin/sh
hadoop fs -cp /data/patents/apat63_99.txt /user/r.shayahmetov/patents/
hadoop fs -cp /data/patents/cite75_99.txt /user/r.shayahmetov/patents/
hive -f create_tables.hive 
