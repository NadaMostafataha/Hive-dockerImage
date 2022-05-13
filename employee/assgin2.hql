create database assign2;
CREATE table assign2.songs (artist_id STRING ,artist_latitude STRING ,artist_location STRING,artist_longitude STRING,artist_name STRING,duration STRING,num_songs STRING,song_id STRING,title STRING,year STRING) partitioned by (year_created  STRING,artist STRING )
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
stored as textfile
TBLPROPERTIES("skip.header.line.count"="1");
load data local inpath 'songs.csv' into table songs partition(year_created = '2022', artist= 'nada');
select * from assign2.songs;
! hadoop dfs -mkdir /user/static;
! hadoop dfs -mkdir /user/static/2021;
! hadoop dfs -mkdir /user/static/2021/the_one;
alter table songs add partition (year_created = '2021' , artist = 'nada')
location '/user/static/2021/the_one';
load data local inpath 'songs.csv' into table songs partition(year_created = '2021', artist ='nada' );
Show partitions songs;
CREATE table assign2.staging (artist_id STRING ,artist_latitude STRING ,artist_location STRING,artist_longitude STRING,artist_name STRING,duration STRING,num_songs STRING,song_id STRING,title STRING,year STRING) 
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
stored as textfile
TBLPROPERTIES("skip.header.line.count"="1");
load data local inpath 'songs.csv' into table assign2.staging;
insert overwrite table songs partition (year_created = '2021', artist ='nada' ) select * from assign2.staging;
truncate table assign2.songs;
select * from assign2.songs;
drop table assign2.songs;
CREATE table songs (artist_id STRING ,artist_latitude STRING ,artist_location STRING,artist_longitude STRING,duration STRING,num_songs STRING,song_id STRING,title STRING) partitioned by (year STRING,artist_name STRING )
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
stored as textfile
TBLPROPERTIES("skip.header.line.count"="1");
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table assign2.songs partition(year,artist_name )
select artist_id ,artist_latitude ,artist_location ,artist_longitude  ,duration ,num_songs ,song_id ,title,year,artist_name from assign2.staging;
Truncate table assign2.songs;
from assign2.staging
insert overwrite table assign2.songs partition(year  = '2005',artist_name)
select artist_id ,artist_latitude ,artist_location ,artist_longitude ,artist_name,duration ,num_songs ,song_id ,title
where year='2005' ;
CREATE TABLE same_stage LIKE assign2.staging 
stored as AVRO;
CREATE TABLE same_stage_PRQ LIKE assign2.staging 
stored as PARQUET;




