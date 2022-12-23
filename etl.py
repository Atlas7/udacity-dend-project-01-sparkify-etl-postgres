import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


# data structure of an incoming Million Song Dataset JSON record
song_data_meta = {
    "dtype": {
        "artist_id": "object",
        "artist_latitude": "float64",
        "artist_location": "object",
        "artist_longitude": "float64",
        "artist_name": "object",
        "duration": "float64",
        "num_songs": "int",        
        "song_id": "object",
        "title": "object",
        "year": "int"
    } 
}

# data structure of an incoming sparkify user session log JSON record
log_data_meta = {
    "dtype": {
        "artist": "object",
        "auth": "object",
        "firstName": "object",
        "gender": "object",
        "itemInSession": "int64",
        "lastName": "object",
        "length": "float64",
        "level": "object",        
        "location": "object",
        "method": "object",
        "page": "object",
        "registration": "object",
        "sessionId": "int64",
        "song": "object",
        "status": "int64",
        "ts": "int64",
        "userAgent": "object",
        "userId": "bigint"
    } 
}

# Million Song Datset JSON fields for parse into psql tables: songs, artists
song_cols = ('song_id', 'title', 'artist_id', 'year', 'duration')
artist_cols = ('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')

# sparkify JSON log file fields for parse into user table
user_cols = ('userId', 'firstName', 'lastName', 'gender', 'level')
time_cols = ("timestamp", "hour", "day", "week", "month", "year", "weekday")


def process_song_file(cur, filepath):
    """process a song JSON file and populate the songs and artists tables.
    
    The input JSON file (at `filepath`) may contains just one line representing the JSON record.
    
    Args:
        cur (psycopg2.extensions.cursor): a psycopg2 curor object 
        filepath (str): the full file path of the song file
        
    Return:
        None
    
    Dependencies:
        This process assumes the `song` and `artist` tables have already been populated
        (by the function `process_song_file`)
        
    Side effects:
        - Populate the `songs` table.
        - Populate the `artists` table.
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # replace all the NaN values with None (because psycopg2 understands Python native None means null.)
    # psycopg2 does not understand (pandas / numpy) NaN and would throw an error if we try to parse it.
    df = df.where(pd.notnull(df), None)

    # insert song record
    song_data = list((df.loc[:, list(song_cols)].values)[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list((df.loc[:, list(artist_cols)].values)[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """process sparkify JSON event log and populate the time, user, and sonplays tables.
    
    The input JSON file (at `filepath`) may contains multiple lines: one line per JSON record.
    
    Args:
        cur (psycopg2.extensions.cursor): a psycopg2 curor object 
        filepath (str): the full file path of the sparkify log file
        
    Return:
        None
    
    Dependencies:
        This process assumes the song and artist tables have already been populated
        (by the function `process_song_file`)
        
    Side effects:
        - Populate the `time` table.
        - Populate the `user` table.
        - Populate the `songplays` table.
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # replace all the NaN values with None (because psycopg2 understands Python native None means null.)
    # psycopg2 does not understand (pandas / numpy) NaN and would throw an error if we try to parse it.
    df = df.where(pd.notnull(df), None)

    # filter by NextSong action
    df = df.loc[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (t.dt.strftime('%Y-%m-%d %H:%M:%S'), t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    time_df = pd.DataFrame(dict(zip(time_cols, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[list(user_cols)]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (time_data[0][index], row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """ Iterate nested JSON file-paths and run a function against each JSON file.
    
    Args:
        - cur (psycopg2.extensions.cursor): a psycopg2 curor object.
        - conn (psycopg2.extensions.connection): a pyscopg2 connection engine
        - filepath (str): the root file-path with nested file structures containing JSON files.
        - func (function): a callback function that we can invoke like this...
            
            func(cur, datafile)
            
            - cur (psycopg2.extensions.cursor): a psycopg2 curor object.
            - datafile (str): a full file-path of a JSON file
        
    Return:
        None
    
    Side effects:
        - postgres activities as defined by `func`
    """
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """The ETL pipelie for sparkifydb"""
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()