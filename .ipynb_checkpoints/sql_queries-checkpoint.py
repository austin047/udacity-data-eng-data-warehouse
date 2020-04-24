import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
                    event_id bigint IDENTITY(0,1)   NOT NULL,
                    artist varchar NULL,
                    auth varchar NOT NULL,
                    first_name varchar NULL,
                    gender varchar NULL,
                    item_in_ession varchar NULL,
                    last_name varchar NULL,
                    length varchar NULL,
                    level varchar NULL,
                    location varchar NULL,
                    method varchar NULL,
                    page varchar NULL,
                    registration varchar NULL,
                    session_id integer NOT NULL,
                    song varchar NULL,
                    status integer NULL,
                    ts bigint NOT NULL,
                    user_agent varchar NULL,
                    user_id int NULL
    )
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs( 
    num_songs integer NOT NULL, 
    artist_id varchar NOT NULL, 
    artist_latitude float, 
    artist_longitude float, 
    artist_location varchar NULL,
    artist_name varchar NOT NULL, 
    song_id varchar NOT NULL,
    title varchar NOT NULL, 
    duration float NOT NULL, 
    year integer NOT NULL ) 
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id integer identity PRIMARY KEY, 
    start_time  timestamp NOT NULL, 
    user_id integer NOT NULL, 
    level varchar NOT NULL, 
    song_id varchar, 
    artist_id varchar distkey, 
    session_id integer NOT NULL, 
    location varchar NOT NULL sortkey, 
    user_agent varchar NOT NULL)
""")


user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id integer PRIMARY KEY, 
    first_name varchar NOT NULL, 
    last_name varchar NOT NULL, 
    gender varchar NOT NULL, 
    level varchar NOT NULL) 
    diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar PRIMARY KEY, 
    title varchar NOT NULL, 
    artist_id varchar NOT NULL, 
    year integer NOT NULL sortkey, 
    duration float NOT NULL)
    diststyle all;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar PRIMARY KEY, 
    name varchar NOT NULL sortkey, 
    location varchar NULL, 
    latitude float NULL, 
    longitude float NULL) 
    diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP  sortkey, 
    hour integer NOT NULL, 
    day integer NOT NULL, 
    week integer NOT NULL,
    month integer NOT NULL, 
    year integer NOT NULL, 
    weekday integer NOT NULL) 
    diststyle all;
""")

# STAGING TABLES


staging_events_copy = ("""
copy staging_events  from '{}/2018/11/2018'
credentials 'aws_iam_role={}' 
format as json '{}'
STATUPDATE ON
region 'us-west-2'
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'),  config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
copy staging_songs  from '{}'
credentials 'aws_iam_role={}' 
format as json 'auto'
STATUPDATE ON
region 'us-west-2'
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))



# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    SELECT DISTINCT timestamp 'epoch' + ts/1000 * interval '1 second' AS start_time,
    e.user_id AS user_id,
    e.level AS level,
    s.song_id AS song_id,
    s.artist_id AS artist_id,
    e.session_id  AS session_id,
    e.location AS location,
    e.user_agent AS user_agent
FROM  staging_events AS e 
JOIN  staging_songs AS s ON s.title=e.song AND s.artist_name=e.artist
WHERE e.page = 'NextSong'
""")


user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level) 
SELECT DISTINCT 
    e.user_id AS user_id, 
    e.first_name AS first_name, 
    e.last_name AS last_name, 
    e.gender AS gender, 
    e.level AS level  
FROM staging_events as e
WHERE page='NextSong'
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT 
    song_id AS song_id, 
    title AS title,
    artist_id AS artist_id,
    year AS year, 
    duration AS duration 
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT
    s.artist_id AS artist_id, 
    s.artist_name AS name, 
    s.artist_location AS location, 
    s.artist_latitude AS latitude, 
    s.artist_longitude AS longitude
FROM staging_songs AS s
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT timestamp 'epoch' + ts/1000 * interval '1 second' AS start_time,
EXTRACT(hour FROM start_time) AS hour,
EXTRACT(day FROM start_time) AS day,
EXTRACT(week FROM start_time) AS week,
EXTRACT(month FROM start_time)  AS month,
EXTRACT(year FROM start_time) AS year,
EXTRACT(weekday FROM start_time) AS weekday
FROM staging_events WHERE page='NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
