DROP TABLE IF EXISTS rankings; CREATE EXTERNAL TABLE rankings (pageURL STRING, pageRank INT, avgDuration INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY "," STORED AS TEXTFILE LOCATION "hdfs://f16/user/skarandikar/bdb_data_latest/rankings";


DROP TABLE IF EXISTS uservisits; CREATE EXTERNAL TABLE uservisits (sourceIP STRING,destURL STRING,visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,languageCode STRING,searchWord STRING,duration INT ) ROW FORMAT DELIMITED FIELDS TERMINATED BY "," STORED AS TEXTFILE LOCATION "hdfs://f16/user/skarandikar/bdb_data_latest/uservisits";


SELECT sourceIP, sum(adRevenue) as totalRevenue, avg(pageRank) as pageRank FROM rankings R JOIN (SELECT sourceIP, destURL, adRevenue FROM uservisits UV WHERE UV.visitDate > "1980-01-01" AND UV.visitDate < "1980-04-01") NUV ON (R.pageURL = NUV.destURL) GROUP BY sourceIP ORDER BY totalRevenue DESC LIMIT 1;
