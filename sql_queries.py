
# Schema for fictitious music store that captures data from event log data.
# SQL queries to drop tables for fresh NoSQL database tables.
# DROP TABLES

music_library_drop = "DROP TABLE IF EXISTS udacity.music_library;"
user_music_library_drop = "DROP TABLE IF EXISTS udacity.user_music_library;"
user_music_history_drop = "DROP TABLE IF EXISTS udacity.user_music_history;"


# SQL queries to create database tables for fact and dimension tables
# CREATE TABLES

music_library_create = ("""CREATE TABLE IF NOT EXISTS udacity.music_library 
                    (artist_name text , song_title text, duration float, session_id int, item_in_session int, 
                    PRIMARY KEY( session_id, item_in_session, artist_name))""")

user_music_library_create = ("""CREATE TABLE IF NOT EXISTS udacity.user_music_library \
                    (artist_name text, user_id int, session_id int , item_in_session int, song_title text, 
                     first_name text, last_name text,  PRIMARY KEY((user_id, session_id), item_in_session))
                     WITH CLUSTERING ORDER BY (item_in_session DESC)""")

user_music_history_create = ("""CREATE TABLE IF NOT EXISTS udacity.user_music_history \
                    (song_title text, first_name text, last_name text, artist_name text, user_id int,
                    PRIMARY KEY (song_title, first_name, last_name ))""")

# SQL queries to insert records into database tables
# INSERT RECORDS


music_library_insert = ("""INSERT INTO udacity.music_library (artist_name, song_title, duration, session_id, item_in_session)
                         VALUES (%s, %s, %s, %s, %s )""")

user_music_library_insert = ("""INSERT INTO udacity.user_music_library (artist_name, user_id, session_id, item_in_session,
                         song_title, first_name, last_name) VALUES (%s, %s, %s, %s, %s, %s, %s ) """)


user_music_history_insert = ("""INSERT INTO udacity.user_music_history (song_title, first_name, last_name, artist_name, user_id) 
                        VALUES (%s, %s, %s, %s, %s ) """)

# SQL qyery to retrive song_id and artist_id from song title, artists name and song length.
# FIND SONGS

song_select = (""" SELECT song_id, a.artist_id FROM songs s INNER JOIN artists a ON s.artist_id = a.artist_id \
                    WHERE title = %s AND name = %s AND duration = %s """)


# SQL queries list to create tables
# SQL queries list to drop tables
# QUERY LISTS

create_table_queries = [music_library_create, user_music_library_create, user_music_history_create]
drop_table_queries = [music_library_drop, user_music_library_drop, user_music_history_drop]