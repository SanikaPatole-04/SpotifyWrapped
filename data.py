### all code

#LIBRARIES AND DATA IMPORT

#import libraries
pip install spotipy
import os
import json
import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyClientCredentialsEDA

#import the streaming data and save as dfs

folder_path = "C:\Users\Sanika Patole\Downloads\my_spotify_data"

dataframes = {}

for file_name in os.listdir(folder_path):
    if file_name.endswith(".json"): 
        file_path = os.path.join(folder_path, file_name)
        with open(file_path, "r", encoding="utf-8") as json_file:
            try:
                data = json.load(json_file)
                df = pd.json_normalize(data)
                dataframes[file_name] = df
            except json.JSONDecodeError as e:
                print(f"Error reading {file_name}: {e}")
                
streaming1_df = dataframes["Streaming_History_Audio_2024_5.json"]
streaming2_df = dataframes['Streaming_History_Audio_2023-2024_4.json']




#CLEAN AND PREP THE DATA

#I'm going to combine the two datasets, filter for the year 2024 since this will be a 2024 wrapped, 
#drop columns I don't need, and get rid of any white noise / lofi music as this is the music I sleep and study to. 
#I'm more interested in the music that I listen to actively

streaming = pd.concat([streaming1_df, streaming2_df], ignore_index=True)

#convert to datetime format
streaming["ts"] = pd.to_datetime(streaming["ts"])

#filter for only 2024
streaming_2024_df = streaming[streaming["ts"].dt.year == 2024]

#drop the columns I don't need
streaming_2024_df = streaming_2024_df.drop(streaming_2024_df.columns[[1, 3, 4, 8, 11, 16, 17, 18]], axis=1)

#arrive at 15000ms as the time that songs were played and skipped
skipped_songs = streaming_2024_df[streaming_2024_df["skipped"]==True]
skipped_songs.ms_played.mean()

#remove rows were ms_played is less than 15,000 ms
streaming_2024_df = streaming_2024_df[streaming_2024_df["ms_played"] > 15000] 

#delete the album_artist name = White Noise Radiance, Lofi Study Music, Yann Tiersen/ Amelie soundtrack because that is my study, chill music

artists_to_remove = ["White Noise Radiance", "LoFi Study Music", "Rain Sounds", "Yann Tiersen", "Philip Glass", 
                     "George Shearing", "Maurice Ravel", "Angels Of Light"]

#filter the df
streaming_2024_df = streaming_2024_df[~streaming_2024_df["master_metadata_album_artist_name"].isin(artists_to_remove)]

#add month to df
streaming_2024_df["month"] = streaming_2024_df.ts.dt.month_name()





#TOP 10 ANALYSIS

#pull song duration from Spotify
#initialize Spotipy with your credentials
client_id = '...'
client_secret = '...'

client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

#list of songs to search for
songs = top_time_listened_songs.master_metadata_track_name.to_list()
artists = top_time_listened_songs.master_metadata_album_artist_name.to_list() 

#search for each song and retrieve its duration
song_data = []

for song, artist in zip(songs, artists):
    # Use both song and artist in the query
    query = f"{song} artist:{artist}"
    result = sp.search(q=query, limit=1, type='track')
    
    if result['tracks']['items']:
        track = result['tracks']['items'][0]
        song_name = track['name']
        artist_name = track['artists'][0]['name']
        duration_ms = track['duration_ms']

        # Append the song details to the list
        song_data.append({
            "Song Name": song_name,
            "Artist Name": artist_name,
            "Duration (ms)": duration_ms
        })
    else:
        # Append a placeholder for songs not found
        song_data.append({
            "Song Name": song,
            "Artist Name": artist,
            "Duration (ms)": None
        })

#create df from the song data
df = pd.DataFrame(song_data)

#I will divide the length of time spent listening to each song by the length of the song 
#this will give me the approx number of times I listened to a song
top_time_listened_songs = streaming_2024_df.groupby(["master_metadata_track_name", "master_metadata_album_artist_name", "master_metadata_album_album_name"])["ms_played"].sum().sort_values(ascending=False).reset_index()

songs_times_df = pd.merge(df, top_time_listened_songs, 
                         left_index=True, right_index=True, how="outer")  

#check for instances where Spotipy pulled wrong song and fix durations if needed
songs_times_df[songs_times_df["Artist Name"] != songs_times_df["master_metadata_album_artist_name"]]

#calcaulate the approx number of times listened
songs_times_df["No. of times listened"] = songs_times_df["ms_played"] / songs_times_df["Duration (ms)"]

#calculate the top songs, artists, albums based on approx no. of times listened
top_songs_by_avg_listens = songs_times_df.sort_values("No. of times listened", ascending=False)

#top 10 songs
top_10_songs = top_songs_by_avg_listens[['Song Name', 'Artist Name', 'No. of times listened']].head(10)

#top 10 artists
top_artists = top_songs_by_avg_listens.groupby(["Artist Name"])["No. of times listened"].sum().sort_values(ascending=False).reset_index().head(10)

#top 10 albums
top_albums = top_songs_by_avg_listens.groupby(["master_metadata_album_album_name", "Artist Name"])["No. of times listened"].sum().sort_values(ascending=False).reset_index().head(10)




#TOP 10 AT A MONTHLY LEVEL (PART 2)

#join song duration to streaming dataset
streaming_duration_2024_df = pd.merge(df, streaming_duration_2024_df, 
                         left_index=True, right_index=True, how="outer")  

#calculate approx no. of times listened
streaming_duration_2024_df["No. of times listened"] = streaming_duration_2024_df["ms_played"] / streaming_duration_2024_df["Duration (ms)"]

#split streaming data by month
dfs_by_month = {month: streaming_duration_2024_df[streaming_duration_2024_df['month'] == month] 
                for month in streaming_duration_2024_df['month'].unique()}

for month in dfs_by_month:
    globals()[f"{month.lower()}_streaming"] = dfs_by_month[month]


#top 10 songs per month
months = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]
top_songs = {}

for month in months:
    df_name = f"{month}_streaming"
    top_songs[month] = eval(df_name).groupby(["master_metadata_track_name", "master_metadata_album_artist_name", "Duration (ms)", "month"])["ms_played"].sum().sort_values(ascending=False).reset_index().head(10)


#top 10 artists per month

top_artists = {}

for month in months:
    df_name = f"{month}_streaming"
    top_artists[month] = eval(df_name).groupby(["master_metadata_album_artist_name", "Duration (ms)", "month"])["ms_played"].sum().sort_values(ascending=False).reset_index().head(10)

#top 10 albums per month

top_albums = {}

for month in months:
    df_name = f"{month}_streaming"
    top_albums[month] = eval(df_name).groupby(["master_metadata_album_album_name", "master_metadata_album_artist_name", "Duration (ms)", "month"])["ms_played"].sum().sort_values(ascending=False).reset_index().head(10)

    
    
#LOOK AT SONG GENRE

#I ran this second Spotipy code in another notebook and imported a csv of the data produced earlier because my computer continuously timed out
client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

songs = top_songs_by_avg_listens.master_metadata_track_name.to_list()
artists = top_songs_by_avg_listens.master_metadata_album_artist_name.to_list() 

# Initialize the list to store song data
song_data = []

# Search for each song and retrieve its genre
for song, artist in zip(songs, artists):
    # Use both song and artist in the query
    query = f"{song} artist:{artist}"
    result = sp.search(q=query, limit=1, type='track')
    
    if result['tracks']['items']:
        track = result['tracks']['items'][0]
        song_name = track['name']
        artist_name = track['artists'][0]['name']
        
        # Retrieve genres of the artist(s)
        artist_id = track['artists'][0]['id']
        artist_info = sp.artist(artist_id)
        genres = artist_info['genres']
        
        # Append the song details to the list
        song_data.append({
            "Song Name": song_name,
            "Artist Name": artist_name,
            "Genres": genres if genres else None  # Handle case where no genres are available
        })
    else:
        # Append a placeholder for songs not found
        song_data.append({
            "Song Name": song,
            "Artist Name": artist,
            "Genres": None
        })

#create df from the collected song data
df = pd.DataFrame(song_data)

#merge data with the avg listens df (using this df as it's less rows to pull data for)
top_songs_by_avg_listens_with_genre = pd.merge(df, top_songs_by_avg_listens, 
                         left_index=True, right_index=True, how="outer")  

top_songs_by_avg_listens_with_genre.groupby("Genres")["No. of times listened"].sum().reset_index().sort_values("No. of times listened", ascending=False).head(20)



#STATS AND OTHER INTERESTING INSIGHTS

#total time listened in 2024
total_time_listening = streaming_2024_df.ms_played.sum() / 3600000

#time range of dataset
streaming_2024_df.ts.min(), streaming_2024_df.ts.max()

#average ms played each day over the course of the year (up to 05/12) 
year_ms = streaming_2024_df.ms_played.sum() / (pd.Timestamp(streaming_2024_df.ts.max()) - pd.Timestamp(streaming_2024_df.ts.min())).days

#minutes per day
avg_mins_day = year_ms / 60000

#hours per day
avg_hours_day = year_ms / 3600000

#look at monthwise listening
monthly_listening = streaming_2024_df.groupby("month")["ms_played"].sum().reset_index()
monthly_listening["hours_listened"] =  monthly_listening["ms_played"] / 3600000
monthly_listening.sort_values("hours_listened", ascending=False)

#add hour of the day 
streaming_2024_df['hour'] = streaming_2024_df.ts.dt.hour

#define time buckets for hourly listening
def time_bucket(hour):
    if 3 <= hour < 11:
        return 'Morning'
    elif 11 <= hour < 18:
        return 'Afternoon'
    else:
        return 'Night'

#apply function
streaming_2024_df['time_bucket'] = streaming_2024_df['hour'].apply(time_bucket)

#aggregate results to look at time of day listening
daily_listening = streaming_2024_df.groupby("time_bucket")["ms_played"].sum().reset_index()
daily_listening["hours_listened"] =  daily_listening["ms_played"] / 3600000
daily_listening.sort_values("hours_listened", ascending=False)
daily_listening["percentage of time over year listening"] = (daily_listening["hours_listened"]/overall_time_listened)*100

#look at hourly listening
hour_listening = streaming_2024_df.groupby("hour")["ms_played"].sum().reset_index()
hour_listening["hours_listened"] =  hour_listening["ms_played"] / 3600000
hour_listening.sort_values("hours_listened", ascending=False)
