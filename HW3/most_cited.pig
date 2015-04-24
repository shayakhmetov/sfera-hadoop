cite = LOAD '/user/r.shayahmetov/patents/cite75_99.txt'  USING PigStorage(',') AS (citing: long, cited: long);
apat = LOAD '/user/r.shayahmetov/patents/apat63_99.txt' USING PigStorage(',') AS (id: long, year: int, gdate: long, appyear: int, country: chararray);
only90 = FILTER apat BY year == 1990 AND country == '"US"';
cited90 = JOIN only90 BY id, cite BY cited;
group_cited = GROUP cited90 BY id;
count_cited = FOREACH group_cited GENERATE group, COUNT(cited90) AS counts; 
most_citied = ORDER count_cited BY counts DESC;
toPrint = LIMIT most_citied 10;
DUMP toPrint;
