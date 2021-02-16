import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3

DATABASE_LOCATION = "sqlite:///played_tracks.sqlite"
USER_ID = "11139922038"
<<<<<<< HEAD
TOKEN = "BQB9LzL490LpB_HFSRp5ombie0FSDVYe2I6d54HRf_dAvLDOZtf31w-CwOpkOUFV7tvEhcutJNuwUfYP9eYGEFcTUKaOposBRmHXDgl6XbBpACQ-BNN-a3CM7Jh7XhpNt1Ljk8f7fTJ91ZFBW1-0PQ"
=======
TOKEN = "BQCzhZLpvtaMQ5JBRtdTP5tr5RwHWPhlpDU4eDC9CJCQobl5ajG_uy6rsWTapY4jbVyhXZU615jneDucxEeGiT2Ptqh_2Sla-rO-WSD4EM9sg-We9fc43tQYinOgPSseHazaM_MWY7yomEh9034BFg"
>>>>>>> 160ccf4f54673a662e390c00b3ae238e868f2f54

    #check if df is empty
def check_if_valid_data(df: pd.DataFrame) -> bool:
    if df.empty:
        print("No songs downloaded")
        return False
    # Primary Key Check
    if pd.Series(df["Played_at"]).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")
    # Check null values.
    if df.isnull().values.any():
        raise Exception ("Null values found")

    # # Check that all timestamps are of yesterday's date
    # yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    # yesterday = yesterday.replace(hour=0, minute=0,second=0,microsecond=0)
    # timestamps = df["Timestamp"].tolist()

    # for timestamp in timestamps:
    #     if datetime.datetime.strptime(timestamp,"%Y-%m-%d") != yesterday:
    #         raise Exception("At least one of the returned songs does not come from within the last 24 hours")
    # return True    



def run_spotify_etl():
    DATABASE_LOCATION = "sqlite:///played_tracks.sqlite"
    USER_ID = "11139922038"
<<<<<<< HEAD
    TOKEN = ""
=======
    TOKEN = "BQCzhZLpvtaMQ5JBRtdTP5tr5RwHWPhlpDU4eDC9CJCQobl5ajG_uy6rsWTapY4jbVyhXZU615jneDucxEeGiT2Ptqh_2Sla-rO-WSD4EM9sg-We9fc43tQYinOgPSseHazaM_MWY7yomEh9034BFg"
>>>>>>> 160ccf4f54673a662e390c00b3ae238e868f2f54
    
    headers = {
        "Accept":"application/json",
        "Content-Type": "application/json",
        "Authorization" : "Bearer {}".format(TOKEN)
    }

    #last 30 days
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=30)
    yesterday_timestamp_unix = int(yesterday.timestamp()) * 1000

    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?limit=10&after={}".format(yesterday_timestamp_unix),headers=headers)
    data = r.json()
    
    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    song_dict = {
        "Song_name":song_names,
        "Artist_name":artist_names,
        "Played_at": played_at_list,
        "Timestamp": timestamps
    }

    song_df = pd.DataFrame(song_dict, columns= ["Artist_name","Song_name","Played_at","Timestamp"])
    print(song_df)
    
    # Validate
    if check_if_valid_data(song_df):
        print("Data valid, proceed to Load stage")

    # Load
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('played_tracks.sqlite')
    cursor = conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS played_tracks(
        Song_name VARCHAR(200),
        Artist_name VARCHAR(200),
        Played_at VARCHAR(200),
        Timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (Played_at)
    )
    """
    cursor.execute(sql_query)
    print("Opened database successfully")
    try:
        song_df.to_sql("played_tracks",engine,index=False,if_exists="append")
    
    except:
        print("Data already exists in the database")

    conn.close()
<<<<<<< HEAD
    print("Database closed successfully")
=======
    print("Database closed successfully")

>>>>>>> 160ccf4f54673a662e390c00b3ae238e868f2f54
