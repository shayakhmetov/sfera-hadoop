#!/bin/sh
INPUT='/data/patents/cite75_99.txt'
OUTPUT='graph'
hadoop fs -rm -r ${OUTPUT}
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
        -numReduceTasks 3 \
        -file initial_mapper.py initial_reducer.py \
        -mapper "initial_mapper.py" \
        -reducer "initial_reducer.py" \
        -input ${INPUT} \
        -output ${OUTPUT}

#hadoop fs -text ${OUTPUT}/part*