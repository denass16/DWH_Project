import configparser
import psycopg2
import pandas as pd
import sql_queries


def load_staging_tables(cur, conn):
    i = 1
    for query in copy_table_queries:
        print("loading staging_table {}...".format(i))
        cur.execute(query)
        conn.commit()
        i += 1
    print("loading complete")


def get_staging_data(conn, table):
    query = "select * from {}".format(table)
    df = pd.read_sql_query(query, conn)
    return df


def process_song_data(cur, conn, func):
    """
    Extracts data from staging_song_table dataframe and inserts songs record into songs table and artists record into artists table.
    
    INPUTS:
    df - DataFrame of song data
    """
    
    #get song data from staging table
    print("getting song data from staging table...")
    table = "staging_songs_table"
    
    df = func(conn, table)
    no_rows = len(df)
    for i in range(no_rows):
        song_data =  df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[i].tolist()
        print("inserting to song_table...")
        cur.execute(song_table_insert, song_data)

        # insert artist record
        artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[i].tolist()
        print("inserting to artist_table...")
        cur.execute(artist_table_insert, artist_data)
    conn.commit()

    
def process_log_data(cur, conn, func):
    """
    Generate time data from log data dataframe and insert into time table.
    Songplay table data are generated from log data and also data obtained from querying both songs and artist tables.
    
    INPUTS:
    df - dataframe of log data
    """
    
    #get logs data from staging table
    table = "staging_events_table"
    print("getting log data from staging table...")
    df = func(conn, table)
    
    # filter by NextSong action
    df = df.loc[df['page']=="NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (t, pd.DatetimeIndex(t).hour, pd.DatetimeIndex(t).day, 
                 pd.DatetimeIndex(t).week, pd.DatetimeIndex(t).month, pd.DatetimeIndex(t).year,  pd.DatetimeIndex(t).weekday)
    column_labels = ('timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame()
    for i in range(len(column_labels)):
        time_df = pd.concat([time_df, pd.DataFrame(list(time_data[i]), columns=[column_labels[i]])], sort=False, axis=1)
    
    print("inserting to time_table...")
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
        
    # load user table
    user_df = df[['user_id', 'first_name', 'last_name', 'gender', 'level']]

    # insert user records
    print("inserting to user_table...")
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
    
    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        print("querying song and artist tables...")
        cur.execute(song_select, (row.song, row.artist, row.length))        
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        print("inserting to songplay_table...")
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.user_id, row.level, songid, artistid, row.session_id, row.location, row.user_agent)
        cur.execute(songplay_table_insert, songplay_data)
    conn.commit()
    
"""
def insert_tables(cur, conn):
    i = 1
    for query in insert_table_queries:
        print("inserting table {}".format(i))
        cur.execute(query)
        conn.commit()
        i += 1
    print("inserting tables complete")
"""    

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("connection successful")
    
    #load_staging_tables(cur, conn)
    process_song_data(cur, conn, func=get_staging_data)
    process_log_data(cur, conn, func=get_staging_data)
    conn.close()


if __name__ == "__main__":
    main()