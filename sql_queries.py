import os
import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create = ("""
create table if not exists staging_events (
  artist varchar(max),
  auth varchar(50),
  firstName varchar(15),
  gender varchar(1),
  itemInSession int,
  lastName varchar(15),
  length numeric(10, 5),
  level varchar(5),
  location varchar(max),
  method varchar(15),
  page varchar(100),
  registration varchar(25),
  sessionId int,
  song varchar(max),
  status int,
  ts bigint,
  userAgent varchar(max),
  userId int
)
""")

staging_songs_table_create = ("""
create table if not exists staging_songs (
  artist_id varchar(18) not null,
  artist_latitude numeric(8, 5),
  artist_longitude numeric(8, 5),
  artist_location varchar(max),
  artist_name varchar(max) not null sortkey,
  song_id varchar(18) not null,
  title varchar(max) not null,
  duration numeric(10, 5),
  year int
)
""")

songplay_table_create = ("""
create table if not exists songplays (
songplay_id int identity(0, 1) not null primary key,
start_time bigint not null,
user_id int not null,
level varchar(10),
song_id varchar(18) not null,
artist_id varchar(18) not null,
session_id int not null,
location varchar(max),
user_agent varchar(max)
);
""")

user_table_create = ("""
create table if not exists users (
user_id int not null primary key,
first_name varchar(15),
last_name varchar(15),
gender varchar(1),
level varchar(5)
)
diststyle all;
""")

song_table_create = ("""
create table if not exists songs (
song_id varchar(18) not null sortkey,
title varchar(max) not null,
artist_id varchar(18) not null,
year int,
duration numeric(10,5) not null
)
diststyle all;
;
""")

artist_table_create = ("""
create table if not exists artists (
artist_id varchar(18) not null sortkey,
name varchar(max) not null,
location varchar(max),
latitude numeric(8, 5),
longitude numeric(8, 5)
)
diststyle all;
""")

time_table_create = ("""
create table if not exists time (
start_time bigint not null sortkey,
hour int not null,
day int not null,
week int not null,
month int not null,
year int not null,
weekday int not null
);
""")

# STAGING TABLES

# load ROLE_ARN from Environment Variable
IAM_ROLE = os.environ.get("DWH_AWS_ROLE_ARN")

staging_events_copy = ("""
copy staging_songs
from {0}
iam_role '{1}'
json 'auto'
COMPUPDATE OFF STATUPDATE OFF
""").format(
    config.get('S3', 'SONG_DATA'),
    IAM_ROLE)

staging_songs_copy = ("""
copy staging_events
from {0}
iam_role '{1}'
json {2}
""").format(
    config.get('S3', 'LOG_DATA'),
    IAM_ROLE,
    config.get('S3', 'LOG_JSONPATH')
)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
select
	cast(se.ts as bigint) as start_time ,
	se.userId as user_id ,
	se.level ,
	sa.song_id ,
	sa.artist_id ,
	se.sessionid as session_id ,
	se.location ,
	se.useragent as user_agent
from
	staging_events as se
join (
	select
		a.artist_id as artist_id,
		s.song_id as song_id,
		a.name as name,
		s.title as title,
		s.duration as duration
	from
		songs as s
	join artists as a on
		a.artist_id = s.artist_id ) as sa on
	se.artist = sa.name
	and se.song = sa.title
	and se.length = sa.duration
""")

user_table_insert = ("""
insert into users (user_id, first_name, last_name, gender, level)
select distinct
  userId
  , firstName
  , lastName
  , gender
  , level
from staging_events
where userId is not null
""")

song_table_insert = ("""
insert into songs (song_id, title, artist_id, year, duration)
select
  song_id
  , title
  , artist_id
  , year
  , duration
from staging_songs
where song_id is not null
""")

artist_table_insert = ("""
insert into artists (artist_id, name, location, latitude, longitude)
select
  artist_id
  , artist_name
  , artist_location
  , artist_latitude
  , artist_longitude
from staging_songs
where artist_id is not null
""")

time_table_insert = ("""
insert into time (start_time, hour, day, week, month, year, weekday)
select
  ts
  , EXTRACT(hour from timestamp 'epoch' + (ts/1000) * interval '1 second')
  , EXTRACT(day from timestamp 'epoch' + (ts/1000) * interval '1 second')
  , EXTRACT(week from timestamp 'epoch' + (ts/1000) * interval '1 second')
  , EXTRACT(month from timestamp 'epoch' + (ts/1000) * interval '1 second')
  , EXTRACT(year from timestamp 'epoch' + (ts/1000) * interval '1 second')
  , EXTRACT(weekday from timestamp 'epoch' + (ts/1000) * interval '1 second')
from (select cast(ts as bigint) as ts from staging_events)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
