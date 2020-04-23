import configparser

# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")
ARN = config.get("CLUSTER", "DWH_ROLE_ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist CHARACTER VARYING,
        auth CHARACTER VARYING,
        firstName CHARACTER VARYING,
        gender CHAR(1),
        itemInSession SMALLINT,
        lastName CHARACTER VARYING,
        length NUMERIC,
        level CHARACTER VARYING,
        location CHARACTER VARYING,
        method CHARACTER VARYING,
        page CHARACTER VARYING,
        registration NUMERIC,
        sessionId BIGINT,
        song CHARACTER VARYING,
        status SMALLINT,
        ts NUMERIC,
        userAgent CHARACTER VARYING,
        userId BIGINT
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs SMALLINT,
        artist_id CHAR(18),
        artist_latitude CHARACTER VARYING,
        artist_longitude CHARACTER VARYING,
        artist_location CHARACTER VARYING,
        artist_name CHARACTER VARYING,
        song_id CHAR (18),
        title CHARACTER VARYING,
        duration NUMERIC,
        year INT
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id BIGINT IDENTITY(0 ,1),
        start_time BIGINT NOT NULL,
        user_id INT, level CHARACTER VARYING,
        song_id CHARACTER VARYING,
        artist_id CHARACTER VARYING,
        session_id BIGINT NOT NULL,
        location CHARACTER VARYING,
        user_agent CHARACTER VARYING
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id BIGINT  PRIMARY KEY NOT NULL,
        first_name CHARACTER VARYING NOT NULL,
        last_name CHARACTER VARYING NOT NULL,
        gender CHARACTER VARYING,
        level CHARACTER VARYING NOT NULL
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id CHARACTER VARYING PRIMARY KEY NOT NULL,
        title CHARACTER VARYING NOT NULL ,
        artist_id CHARACTER VARYING NOT NULL,
        year SMALLINT,
        duration NUMERIC
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
    artist_id CHARACTER VARYING PRIMARY KEY NOT NULL,
    name CHARACTER VARYING NOT NULL,
    location CHARACTER VARYING,
    latitude NUMERIC,
    longitude NUMERIC
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time BIGINT PRIMARY KEY NOT NULL,
        hour SMALLINT NOT NULL,
        day SMALLINT NOT NULL,
        week SMALLINT NOT NULL,
        month SMALLINT NOT NULL,
        year SMALLINT NOT NULL,
        weekday SMALLINT NOT NULL
    )
""")

# STAGING TABLES COPY FRON S3

staging_events_copy = ("""
    copy staging_events from '{}' iam_role '{}' format as json '{}' region 'us-west-2'
""").format(config.get("S3", "LOG_DATA"), ARN, config.get("S3", "LOG_JSONPATH"))

staging_songs_copy = ("""
    copy staging_songs from '{}'
    iam_role '{}'
    json 'auto'
    region 'us-west-2'
""").format(config.get("S3", "SONG_DATA"), ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id,session_id, location, user_agent) 
SELECT  se.ts,
        se.userId as user_id,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.sessionId as session_id,
        se.location,
        se.userAgent as user_agent
FROM staging_events se LEFT JOIN staging_songs ss ON se.song=ss.title AND se.artist=ss.artist_name
WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users SELECT se.userId, se.firstName, se.lastName, se.gender, se.level
    FROM staging_events se
    JOIN (
        SELECT max(ts) AS ts, userId
        FROM staging_events
        WHERE page = 'NextSong'
        GROUP BY userId
    ) recent on se.userId = recent.userId and se.ts = recent.ts
""")

# Alternative case to insert the most recent user value using redshift's row_number and partition by logic
user_table_insert__with_row = ("""
INSERT INTO users 
SELECT * FROM (
    SELECT userId,firstName,lastName,gender,level, ROW_NUMBER() OVER (PARTITION BY userI ORDER BY ts DESC) AS user_id_recent
    FROM staging_events se
     WHERE page = 'NextSong'
    ) ordered
WHERE ordered.user_id_recent = 1    
""")



song_table_insert = ("""
INSERT INTO songs SELECT DISTINCT song_id,title,artist_id,year,duration FROM staging_songs
""")

artist_table_insert = ("""
 INSERT INTO  artists SELECT DISTINCT artist_id, artist_name as name, artist_location as location, artist_latitude as latitude, artist_longitude as longitude FROM staging_songs
""")

time_table_insert = ("""
  INSERT INTO time
    SELECT  
        t.ts,
        EXTRACT(HOUR FROM t.start_time) AS hour,
        EXTRACT(DAY FROM t.start_time) AS day,
        EXTRACT(WEEK FROM t.start_time) AS week,
        EXTRACT(MONTH FROM t.start_time) AS month,
        EXTRACT(YEAR FROM t.start_time) as year,
        EXTRACT(WEEKDAY FROM t.start_time) as weekday
        
    FROM (SELECT DISTINCT TIMESTAMP 'epoch' + ts / 1000 * interval '1 second' AS start_time,ts FROM staging_events 
        WHERE page = 'NextSong'
    ) t
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,time_table_insert]
