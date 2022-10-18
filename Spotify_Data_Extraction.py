import datetime
import sqlite3
from datetime import datetime
from datetime import timedelta
import pandas as pd
import requests
import sqlalchemy
import os.path
from sqlalchemy import create_engine
import pymysql


def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False
    # VERY IMPORTANT STEP: Key Check
    if pd.Series(df["played_at"]).is_unique:
        pass
    else:
        raise Exception("Primary Key Check is violated")
    # Nulls can not be in Spotify Data, there is a check:
    if df.isnull().values.any():
        raise Exception("Null values found")
    # Check that all timestamps are yesterday's date
    yesterday = datetime.now() - timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    exceptions = 0
    while True:
        timestamps = df["timestamp"].tolist()
        for timestamp in timestamps:
            if datetime.strptime(timestamp, "%Y-%m-%d") != yesterday:
                exceptions += 1
                print("At least one of the returned songs does not come from within the last 24 hours")
            else:
                break
        break
    if exceptions > 0:
        with open('exceptions', 'w', encoding='utf-8') as f:
            f.write(str(exceptions))
        return False
    if exceptions == 0:
        return True


def run_spotify_etl():
    # put your user_id and user_secret here in ""
    auth = ("", "")
    # put your refresh_token down there in empty ""
    params = {
        "grant_type": "refresh_token",
        "refresh_token": ""
    }

    url = "https://accounts.spotify.com/api/token"

    ret = requests.post(url, auth=auth, data=params)

    print(ret)
    token = ret.json()['access_token']

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=token)
    }

    today = datetime.now()
    today = today.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000 + 10800000

    limit_of_data_limit = 0
    data_limit = 50
    exceptions = 0

    if os.path.isfile('limit_of_data_limit'):
        with open('limit_of_data_limit', 'r', encoding='utf-8') as f:
            limit_of_data_limit = f.read()
            limit_of_data_limit = int(limit_of_data_limit)
            data_limit = limit_of_data_limit
            print(limit_of_data_limit)
    else:
        pass
    if os.path.isfile('exceptions'):
        with open('exceptions', 'r', encoding='utf-8') as f:
            exceptions = f.read()
            exceptions = int(exceptions)
            data_limit = data_limit - exceptions
            print(exceptions)
    else:
        pass

    if os.path.isfile('exceptions'):
        os.remove(r"/home/exceptions")

    if data_limit == 0:
        raise Exception("All the tracks are today's date")

    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?limit={data_amount}&after={time}".format(time=yesterday_unix_timestamp, data_amount=data_limit), headers=headers)

    data = r.json()

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    os.chdir(r"/home")

    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])
        limit_of_data_limit += 1

    with open('limit_of_data_limit', 'w', encoding='utf-8') as f:
        f.write(str(limit_of_data_limit))

    song_dict = {
        "song_name": song_names,
        "artist_name": artist_names,
        "played_at": played_at_list,
        "timestamp": timestamps
    }

    song_df = pd.DataFrame(song_dict, columns=["song_name", "artist_name", "played_at", "timestamp"])
    pd.set_option('display.max_columns', None)
    print(song_df)

    if check_if_valid_data(song_df):
        print("Data valid, proceed to Load stage")
    else:
        run_spotify_etl()
        return "process is done"

    if os.path.isfile('limit_of_data_limit'):
        os.remove(r"/home/limit_of_data_limit")
    # fill next four empty "" by your data
    try:
        hostname = ""
        dbname = ""
        uname = ""
        pwd = ""
        engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=hostname, db=dbname, user=uname, pw=pwd))
        song_df.to_sql('my_played_tracks', engine, index=False, if_exists="append")
        print("Close database successfully")
    except:
        print("Data already exists in the database")


run_spotify_etl()
