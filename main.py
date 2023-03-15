import psycopg2 as pg
import pandas as pd
import requests
from configparser import ConfigParser
from datetime import datetime, timedelta
from sqlalchemy import create_engine

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
    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        timestamp = str(timestamp)[0:10]
        if timestamp != yesterday:
            raise Exception("At least one of the returned songs does not have yesterday's timestamp")

    return True

if __name__=="__main__":

    config = ConfigParser()
    config.read(CONFIG_FILE)

    #Get Spotify API Configs
    user_id = config['API'].get('USER_ID')
    token = config['API'].get('TOKEN')
    endpoint = config['API'].get('ENDPOINT')
    
    # Get Database Configs

    host = config['POSTGRES'].get('HOST')
    port = config['POSTGRES'].get('PORT')
    database = config['POSTGRES'].get('DATABASE')
    user = config['POSTGRES'].get('USER')
    password = config['POSTGRES'].get('PASSWORD')


    headers ={
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : f"Bearer {token}"
    }

    today = datetime.now()
    yesterday = today - timedelta(days=1)
    print(yesterday)
    the_day_before_yesterday = today - timedelta(days=2)
    the_day_before_yesterday_unix_timestamp = int(the_day_before_yesterday.timestamp()) * 1000

    #print(today,yesterday)
    url = endpoint + f'?after={the_day_before_yesterday_unix_timestamp}'
    response = requests.get(url,headers=headers)

    data = response.json()

    #print(data)
    song_names = []
    artist_names = []
    played_at = []
    timestamps = []

    for song in data['items']:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["artists"][0]["name"])
        played_at_temp = datetime.strptime(song["played_at"], '%Y-%m-%dT%H:%M:%S.%fZ')
        played_at.append(played_at_temp.strftime('%Y-%m-%d %H:%M:%S'))
        timestamps.append(song["played_at"][0:10])

    song_dict = {
        "song_name" : song_names,
        "artist_name" : artist_names,
        "played_at": played_at,
        "timestamp" : timestamps
    }

    song_df = pd.DataFrame(song_dict,columns=["song_name","artist_name","played_at","timestamp"])

    print(song_df)

    if check_if_valid_data(song_df):
        print("Data Valid, proceed to Load Stage.")

    #Load
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")
    yesterday_datetime = yesterday.strftime('%Y%m%d')
    print(yesterday_datetime)
    # Insert the data from the DataFrame to the database
    song_df.to_sql(f"spotify_tracks_{yesterday_datetime}", engine, if_exists="fail", index=False)
    #song_df.to_sql('my_spotify_tracks', engine, if_exists='fail', index=True, index_label='played_at')
