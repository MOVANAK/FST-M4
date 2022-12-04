-- Load data from HDFS
inputDialouges = LOAD 'hdfs://localhost:9000/user/root/inputs/Episode IV_dialouges.txt' USING PigStorage('\t') AS (name: chararray, line: chararray); 

-- Filter out the first 2 lines
ranked = RANK inputDialouges;
OnlyDialouges = FILTER ranked BY (rank_inputDialouges > 2);

-- Group by name
groupByName = GROUP OnlyDialouges BY name;

--Count the number of lines by each character
names = FOREACH groupByName GENERATE $0 as name, COUNT($1) as no_of_lines;
namesOrdered = ORDER names BY no_of_lines DESC;

Store result in HDFS.
STORE namesOrdered INTO 'hdfs://localhost:9000/user/root/outputs/episode IVOutput' USING PigStorage('\t');


