CREATE table events(artist STRING,auth STRING,firstName STRING,gender STRING,itemInSession STRING,lastName STRING, length FLOAT,level STRING,location STRING,method STRING,page STRING,registration FLOAT,sessionId INT,song STRING,status INT,ts FLOAT,userAgent STRING,userId INT)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
stored as textfile
TBLPROPERTIES("skip.header.line.count"="1");
Load data local inpath 'events.csv' into table events;
select userId,sessionId,first_value(song) OVER(PARTITION BY sessionId ORDER BY ts) AS f_song, last_value(song) OVER(PARTITION BY sessionId ORDER BY ts) AS l_song
FROM events;
select x.userId,Rank() Over(order by x.cn_song desc) as rank
from 
(select userId,count (distinct song) as cn_song
 from events
 where page ='NextSong'
 group by userId
) x
order by rank desc;
select x.userId,ROW_NUMBER() Over(order by x.cn_song desc) as rank
from 
(select userId,count (distinct song) as cn_song
 from events
where page ='NextSong'
 group by userId
) x
order by rank desc;
select location,artist,count(page) as cn_song from events 
where page ='NextSong'
group by location,artist
grouping sets ((location,artist),location);
select location,artist,count(page) as cn_song from events 
where page ='NextSong'
group by location,artist
grouping sets ((location,artist),location);
select sessionId, userId,lead(song) over
(partition by userId order by sessionId desc) from events
 order by sessionId desc;
Select userId,song,ts from events ORDER BY userId,song,ts;
Select userId,song,ts from events CLUSTER BY userId,song,ts;



