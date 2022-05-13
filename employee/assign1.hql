CREATE DATABASE assign1;
describe database assign1;
!hadoop fs -mkdir /hp_db;
!hadoop fs -mkdir /hp_db/assign1_loc;
create database assign1_loc
location '/hp_db/assign1_loc';
create table assign1.assign1_intern_tab(name STRING,age INT,job_title STRING,mgr_id INT,loc STRING,salary FLOAT,no INT)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
stored as textfile
TBLPROPERTIES("skip.header.line.count"="1");
describe formatted assign1.assign1_intern_tab;
!hadoop dfs -put employee.csv /user/hive/warehouse/assign1.db/assign1_intern_tab;
Load data local inpath 'employee.csv' into table assign1.assign1_intern_tab;
select * from assign1.assign1_intern_tab
limit 10;
!hadoop fs -mkdir /hp_db/assign1_loc;
CREATE external table assign1_loc.assign1_intern_tab (name STRING,age INT,job_title STRING,mgr_id INT,loc STRING,salary FLOAT,no INT)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
stored as textfile
location '/hp_db/assign1_loc'
TBLPROPERTIES("skip.header.line.count"="1");
describe formatted assign1_loc.assign1_intern_tab;
Load data local inpath 'employee.csv' into table assign1_loc.assign1_intern_tab;
DROP TABLE IF EXISTS assign1.assign1_intern_tab;
DROP TABLE IF EXISTS assign1_loc.assign1_intern_tab;
create table assign1.assign1_intern_tab(name STRING,age INT,job_title STRING,mgr_id INT,loc STRING,salary FLOAT,no INT)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
stored as textfile
TBLPROPERTIES("skip.header.line.count"="1");
CREATE external table assign1_loc.assign1_intern_tab (name STRING,age INT,job_title STRING,mgr_id INT,loc STRING,salary FLOAT,no INT)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
stored as textfile
location '/hp_db/assign1_loc'
TBLPROPERTIES("skip.header.line.count"="1");
describe formatted assign1.assign1_intern_tab;
describe formatted assign1_loc.assign1_intern_tab;
CREATE table assign1.staging (name STRING, age INT,job_title STRING,mgr_id INT,loc STRING,salary FLOAT,no INT) row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
stored as textfile;
Load data local inpath 'employee.csv' into table assign1.staging;
insert into table assign1.staging select * from assign1_loc.assign1_intern_tab;
insert into table assign1.staging select * from assign1.assign1_intern_tab;
!hdfs dfs -ls /hp_db/assign1_loc;
!hdfs dfs -ls /user/hive/warehouse/assign1.db/assign1_intern_tab;
! wc -l  /employee/songs.csv; 
!hadoop fs -mkdir /user/song;
CREATE external table assign1.song (artist_id STRING ,artist_latitude STRING ,artist_location STRING,artist_longitude STRING,artist_name STRING,duration STRING,num_songs STRING,song_id STRING,title STRING,year STRING)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
 stored as textfile
location'/user/song'
TBLPROPERTIES("skip.header.line.count"="1");
! hadoop dfs -put /employee/songs.csv /user/song;
!cat /employee/songs.csv;
hive -f script.hql;
alter table assign1_intern_tab rename to assign1_loc.assign1_intern_tab;
describe formatted assign1_loc.assign1_intern_tab;


