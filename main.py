import psycopg2 as pg
from psycopg2 import sql
import pandas as pd
import requests
from configparser import ConfigParser
from datetime import datetime, timedelta
import pytz

CONFIG_FILE = 'config.ini'

def check_if_valid_data(df: pd.DataFrame) -> bool:
    # if the df is empty
    if df.empty:
        print("No Songs downloaded. Finished Execution")
        return False
    if not pd.Series(df["played_at"]).is_unique:
        #print("Primary Check Failed. Duplicate Data Found.")
        raise Exception("Primary Check Failed. Duplicate Data Found.")
    if df.isnull().values.any():
        raise Exception("Null Value Check Failed. Null Value Found.")
    
    # Check that all timestamps are of yesterday's date
    yesterday = datetime.now() - timedelta(days=1)
    yesterday = str(yesterday)[0:10]
    played_dates = df["played_date"].tolist()
    for played_date in played_dates:
        played_date = str(played_date)[0:10]
        if played_date != yesterday:
            raise Exception("At least one of the returned songs does not have yesterday's timestamp")

    return True

def insert_data(params : dict,song_df : pd.DataFrame) -> bool:
    print("Inserting Data into PostgreSQL DB.")
    try:
        # Establish a connection to the PostgreSQL database
        connection = pg.connect(**params)
        # Create a cursor object
        cursor = connection.cursor()
        # Iterate over the rows of the DataFrame and insert data into the PostgreSQL table
        for index, row in song_df.iterrows():
            # Define the insert statement with placeholders for the data
            insert_statement = sql.SQL("""
                INSERT INTO public.spotify_tracks_master (song_name, artist_name, played_at, played_date)
                VALUES ({}, {}, {}, {})
                ON CONFLICT (played_at) DO UPDATE SET
                song_name = EXCLUDED.song_name,
                artist_name = EXCLUDED.artist_name,
                played_date = EXCLUDED.played_date;
                """).format(
                sql.Literal(row['song_name']),
                sql.Literal(row['artist_name']),
                sql.Literal(row['played_at']),
                sql.Literal(row['played_date'])
            )

            # Execute the insert statement
            cursor.execute(insert_statement)
    except Exception as e:
        print(e)
        return False
    finally:
        # Commit the changes to the database
        connection.commit()
        # Close the cursor and connection
        cursor.close()
        connection.close()
        return True



if __name__=="__main__":

    config = ConfigParser()
    config.read(CONFIG_FILE)

    #Get Spotify API Configs
    user_id = config['API'].get('USER_ID')
    token = config['API'].get('TOKEN')
    endpoint = config['API'].get('ENDPOINT')
    
    # Get Database Configs
    params = {
    'host' : config['POSTGRES'].get('HOST'),
    'port' : config['POSTGRES'].get('PORT'),
    'database' : config['POSTGRES'].get('DATABASE'),
    'user' : config['POSTGRES'].get('USER'),
    'password' : config['POSTGRES'].get('PASSWORD')
    }

    headers ={
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : f"Bearer {token}"
    }

    # Get the timestamps for yesterday's start and end times
    tz = pytz.timezone('Asia/Kolkata')

    yesterday_start = (datetime.now(tz) - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_start_unix_timestamp = int(yesterday_start.timestamp()) * 1000
    yesterday_end = (datetime.now(tz) - timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=999999)
    yesterday_end_unix_timestamp = int(yesterday_end.timestamp()) * 1000

    #print(yesterday_start,yesterday_start_unix_timestamp)
    #url = endpoint + f"&after={yesterday_start_unix_timestamp}"
    url = endpoint + f"&before={yesterday_end_unix_timestamp}"
    response = requests.get(url,headers=headers)

    data = response.json()

    #print(data)
    song_names = []
    artist_names = []
    played_at = []
    timestamps = []

    for song in data["items"]:
        if song["played_at"][0:10] ==  str(yesterday_start)[0:10]:
        # if yesterday_start <= played_at_temp <= yesterday_end:
            song_names.append(song["track"]["name"])
            artist_names.append(song["track"]["artists"][0]["name"])
            played_at_temp = datetime.strptime(song["played_at"], '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=pytz.utc).astimezone(tz)
            played_at.append(played_at_temp.strftime('%Y-%m-%d %H:%M:%S'))
            timestamps.append(song["played_at"][0:10])

    song_dict = {
        "song_name" : song_names,
        "artist_name" : artist_names,
        "played_at": played_at,
        "played_date" : timestamps
    }

    song_df = pd.DataFrame(song_dict,columns=["song_name","artist_name","played_at","played_date"])

    #print(song_df)
    if check_if_valid_data(song_df):
        print("Data Valid, proceed to Load Stage.")

    #Load
    if insert_data(params,song_df):
        print("Data Loaded Successfully.")
    else:
        print("Failed to Load the Data")