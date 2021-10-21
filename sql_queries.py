import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table;"
user_table_drop = "DROP TABLE IF EXISTS user_table;"
song_table_drop = "DROP TABLE IF EXISTS song_table;"
artist_table_drop = "DROP TABLE IF EXISTS artist_table;"
time_table_drop = "DROP TABLE IF EXISTS time_table;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events_table(
    artist varchar,
    auth varchar,
    first_name varchar,
    gender varchar(1),
    item_in_session int,
    last_name varchar,
    length float,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    reg float,
    session_id int,
    song varchar,
    status int,
    ts bigint,
    user_agent varchar,
    user_id varchar);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs_table(
    num_songs int,
    artist_id varchar,
    artist_latitude float,
    artist_longitude float,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration float,
    year int);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay_table(
    songplay_id int IDENTITY(1, 1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id int,
    level varchar,
    song_id varchar,
    artist_id varchar,
    session_id int,
    location varchar,
    user_agent varchar);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS user_table(
    user_id int PRIMARY KEY,
    first_name varchar,
    last_name varchar,
    gender varchar(1),
    Level varchar);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song_table(
    song_id varchar PRIMARY KEY,
    title varchar,
    artist_id varchar,
    year int,
    duration float);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist_table(
    artist_id varchar PRIMARY KEY,
    artist_name varchar,
    location varchar,
    latitude float,
    longitude float);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time_table(
    start_time TIMESTAMP PRIMARY KEY,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday int);
""")

# STAGING TABLES
ARN = config.get("IAM_ROLE","ARN")
staging_songs_copy = ("""
    copy staging_songs_table from 's3://udacity-dend/song_data'
    credentials 'aws_iam_role={}'    
    format as json 'auto'
    region 'us-west-2';
""").format(ARN)

staging_events_copy = ("""
    copy staging_events_table from 's3://udacity-dend/log_data'
    credentials 'aws_iam_role={}'    
    json 's3://udacity-dend/log_json_path.json'
    region 'us-west-2';
""").format(ARN)

# FINAL TABLES

songplay_table_insert = ("""
        INSERT INTO songplay_table (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        VALUES (%s, %s, %s, %s, %s,%s, %s, %s)
""")

user_table_insert = ("""
        INSERT INTO user_table
        VALUES (%s, %s, %s, %s, %s)
""")

song_table_insert = ("""
        INSERT INTO song_table
        VALUES (%s, %s, %s, %s, %s)
""")

artist_table_insert = ("""
        INSERT INTO artist_table
        VALUES (%s, %s, %s, %s, %s)
""")

time_table_insert = ("""
        INSERT INTO time_table
        VALUES (%s, %s, %s, %s, %s, %s, %s)
""")

song_select = ("SELECT song_id, song_table.artist_id FROM (song_table JOIN artist_table ON song_table.artist_id=artist_table.artist_id) \
                WHERE title=%s AND artist_name=%s AND duration=%s;")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_songs_copy, staging_events_copy]
#insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
