import streamlit as st
import googleapiclient.discovery
import json
import pandas as pd
from pymongo import MongoClient
from googleapiclient.errors import HttpError
import pymysql
from sqlalchemy import create_engine
import isodate

st.markdown("## :red[YouTube] Data Harvesting and Warehousing" ) #title of the project

with st.sidebar:
      
      st.header("Import youtube channel data to MongoDB Using Google API") 

      channel_id = st.text_input("Channel_id:",placeholder="Enter your channel id") #for user to enter the channel id

      api = st.text_input("API_key:",placeholder="Enter your API key") # for user to enter the channel id

      if channel_id and api:
            st.success('Thanks for your input, Please select the below option to Import the data in MongoDB')


      import_data_to_mongodb = st.button ("Import Data to MongoDB") # button for import the data into MongoDB

      if import_data_to_mongodb: # script for the "Import data to MongoDB" button
            ch_id= channel_id
            api_key = api

            api_service_name = "youtube"
            api_version = "v3"

            youtube = googleapiclient.discovery.build(
                  api_service_name, api_version, developerKey=api_key)
            
            def channel_data(channel_id): # function to get channel information

                  request_channel = youtube.channels().list(
                              part="snippet,contentDetails,statistics,status",
                              id=channel_id
                  )
                  response_channel = request_channel.execute()
                  channel_name = response_channel['items'][0]['snippet']['title']
                  channel_table = {
                        "channel_id" : response_channel['items'][0]['id'],
                        "channel_name" : channel_name,
                        "channel_description" : response_channel['items'][0]['snippet']['description'],
                        "channel_type" : "cooking" if "cooking" or "cook" in response_channel["items"][0]["snippet"]["description"] else None,
                        "channel_views" : int(response_channel['items'][0]['statistics']['viewCount']),
                        "subcribercount" : int(response_channel['items'][0]['statistics']['subscriberCount']),
                        "total_video_count" : int(response_channel['items'][0]['statistics']['videoCount']),
                        "channel_status" : response_channel['items'][0]['status']['privacyStatus']
                        }       
                  return channel_table
            

            def pl_table(channel_id): # function to get playlist table
                  playlist_items = ["channel_id","playlist_id","playlist_name"]
                  playlist = {i:[] for i in playlist_items}

                  request_playlist_table = youtube.playlists().list(
                        part="snippet,contentDetails",
                        channelId=channel_id,
                        maxResults=50
                  )
                  response_playlist_table = request_playlist_table.execute()
                  for i in range(len(response_playlist_table['items'])):
                              playlist['channel_id'].append(response_playlist_table['items'][i]['snippet']['channelId'])
                              playlist['playlist_id'].append(response_playlist_table['items'][i]['id'])
                              playlist['playlist_name'].append(response_playlist_table['items'][i]['snippet']['title'])
                  
                  # Check if the playlist is empty
                  if not response_playlist_table['items']:
                        return pl_table

                  nextpagetoken= response_playlist_table.get("nextPageToken")
                  next_page = True
                  while next_page:
                        if nextpagetoken is None:
                              next_page = False
                        else:
                              request_playlist_table = youtube.playlists().list(
                              part="snippet,contentDetails",
                              maxResults=50,
                              pageToken=nextpagetoken,
                              channelId=channel_id
                        )
                              response_playlist_table = request_playlist_table.execute()
                              for i in range(len(response_playlist_table['items'])):
                                    playlist['channel_id'].append(response_playlist_table['items'][i]['snippet']['channelId'])
                                    playlist['playlist_id'].append(response_playlist_table['items'][i]['id'])
                                    playlist['playlist_name'].append(response_playlist_table['items'][i]['snippet']['title'])

                        
                              nextpagetoken= response_playlist_table.get("nextPageToken")
                        return playlist
                  

            def video_list(video_id): # function to get a video details

                  request_video_list = youtube.videos().list(
                  part="snippet,contentDetails,statistics",
                  id=video_id
                  )
                  response_video_list = request_video_list.execute()
                  
                  if response_video_list ["pageInfo"]['totalResults'] != 0:
                        video_data = {        
                        
                              'video_id': response_video_list['items'][0]['id'],
                              'video_name' : response_video_list['items'][0]['snippet']['title'],
                              'video_description': response_video_list['items'][0]['snippet']['description'],
                              'published_date': response_video_list['items'][0]['snippet']['publishedAt'],
                              'view_count': response_video_list['items'][0]['statistics']['viewCount'],
                              'like_count': response_video_list['items'][0]['statistics'].get('likeCount', 0),
                              'favorite_count': response_video_list['items'][0]['statistics'].get('favoriteCount', 0),
                              'comment_count': response_video_list['items'][0]['statistics'].get('commentCount', 0),
                              'duration': response_video_list['items'][0]['contentDetails']['duration'],
                              'thumbnail': response_video_list['items'][0]['snippet']['thumbnails']['default']['url']

                        }
                        
                        return video_data
                  else:
                        return None      

            # function to get video id with playlist id

            def playlist_id_and_video_id(playlist_id):
                  vi_and_pl_ids = ["video_id", "playlist_id"]
                  playlist_id_and_video_id = {i:[] for i in vi_and_pl_ids}

                  request_video_id = youtube.playlistItems().list(
                        part="snippet,contentDetails",
                        maxResults=50,
                        playlistId=playlist_id
                  )
                  response_video_id = request_video_id.execute()
                  for i in range(len(response_video_id['items'])):
                              playlist_id_and_video_id['video_id'].append(response_video_id['items'][i]["contentDetails"]["videoId"])
                              playlist_id_and_video_id['playlist_id'].append(response_video_id['items'][i]['snippet']['playlistId'])
                  
                  # Check if the playlist is empty
                  if not response_video_id['items']:
                        return playlist_id_and_video_id

                  nextpagetoken= response_video_id.get("nextPageToken")
                  next_page = True
                  while next_page:
                        if nextpagetoken is None:
                              next_page = False
                        else:
                              request_video_id = youtube.playlistItems().list(
                              part="snippet,contentDetails",
                              maxResults=50,
                              pageToken=nextpagetoken,
                              playlistId=playlist_id
                        )
                              response_video_id = request_video_id.execute()
                              for i in range(len(response_video_id['items'])):
                                    playlist_id_and_video_id['video_id'].append(response_video_id['items'][i]["contentDetails"]["videoId"])
                                    playlist_id_and_video_id['playlist_id'].append(response_video_id['items'][i]['snippet']['playlistId'])
                        
                              nextpagetoken= response_video_id.get("nextPageToken")
                        return playlist_id_and_video_id
   
            # function to get comment data

            def comment_thread(video_id):

                  comment_info = ['comment_id','comment_text','video_id','comment_author','comment_published_date']
                  comment_data = {i: [] for i in comment_info}

                  try:
                        request_comment = youtube.commentThreads().list(
                        part="snippet,replies",
                        videoId=video_id
                        )

                        response_comment = request_comment.execute()
                        result = response_comment["pageInfo"]['totalResults']

                        nextpagetoken = None

                        while True:
                              request_comment = youtube.commentThreads().list(
                                    part="snippet,replies",
                                    maxResults=result,
                                    pageToken=nextpagetoken,
                                    videoId=video_id
                              )
                              response_comment = request_comment.execute()
                              result = response_comment["pageInfo"]['totalResults']
                              for i in range(result):
                                    comment_data['comment_id'].append(response_comment['items'][i]['id'])
                                    comment_data['video_id'].append(response_comment['items'][i]['snippet']['videoId'])
                                    comment_data["comment_text"].append(response_comment['items'][i]['snippet']['topLevelComment']['snippet']['textDisplay'])
                                    comment_data['comment_author'].append(response_comment['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'])
                                    comment_data['comment_published_date'].append(response_comment['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'])
                              if nextpagetoken is None:
                                    break
                  except HttpError as e:
                        pass  # Skip video if comments are disabled

                  return comment_data 
            

            channel_table = channel_data(ch_id) # variable to store the channel data

            #creating the playlist data
            playlist_table = pl_table(ch_id) 
            pl_df = pd.DataFrame(playlist_table)

            #listing out the playlist id
            playlist_id = list(pl_df['playlist_id'])
            
            #to getting the video id from playlist id

            video_id_df = [playlist_id_and_video_id(i) for i in playlist_id]
            video_id_df1 =pd.DataFrame(video_id_df)
            video_id_df2 = video_id_df1['video_id'].explode()
            video_id_df3 = video_id_df1['playlist_id'].explode()
            video_id_df4 = [video_id_df2,video_id_df3]
            video_id_df5 = pd.concat(video_id_df4,axis=1).reset_index(drop=True)
            video_id_df6 = video_id_df5.dropna()
            video_id_df7 = video_id_df6.drop_duplicates("video_id").reset_index(drop=True)
            video_id = list(video_id_df6['video_id'])

            # to get video data

            video_table = {"video_id_"+str(i+1):video_list(video_id[i]) for i in range(len(video_id))}

            video_df1 = pd.DataFrame(video_table).transpose()
            video_df2 = video_df1.dropna()

            # merging two datas to link video id with its respective playlist id 
            video_table = pd.merge(video_id_df7,video_df2, on="video_id",how="inner")

            # convert the dataframe to json for storing into MongoDB

            video_data_json = video_table.to_json()
            video_data= json.loads(video_data_json)

            # to get comment data

            comment_table = {"comment_"+str(i+1):comment_thread(video_id[i]) for i in range(len(video_id))}    

            channel_data = { "channel": [channel_table],"playlist_data": [playlist_table],"video_data": [video_data],"comment_table": [comment_table]}

            # Setting up MongoDB connection
            mongo_client = MongoClient("mongodb://localhost:27017/") 

            db_name = "Youtube_Data"

            collection_name = channel_table["channel_name"].replace(' ', '_')

            db = mongo_client[db_name]

            existing_collections = db.list_collection_names()

            # To store channel data
            if collection_name not in existing_collections:
                  collection_channel = db[collection_name]
                  collection_channel.insert_one(channel_data)

            else:
                  collection_channel = db[collection_name]
                  collection_channel.replace_one({},channel_data)

            # Close the MongoDB connection
            mongo_client.close()

            if channel_id and api:
                  st.success('Succesfully stored data in MongoDB')
            else:
                  if not channel_id:
                        st.warning("Please enter channel id")
                  if not api:
                        st.warning("Please enter your api key")


# Set up MongoDB connection
mongo_client = MongoClient("mongodb://localhost:27017/") 
db = mongo_client["Youtube_Data"]
existing_collections = db.list_collection_names()

def channel_SQL_table(collection_channel):
    df_channel = pd.DataFrame(list(collection_channel.find({},{"_id":0})))
    channel_table = pd.DataFrame(df_channel['channel'][0])
    return channel_table

def playlist_SQL_table(collection_channel):
    df_channel = pd.DataFrame(list(collection_channel.find({},{"_id":0})))
    playlist_table_df1 = pd.DataFrame(df_channel['playlist_data'][0]).transpose()
    playlist_table = playlist_table_df1[0].apply(pd.Series).transpose()
    return playlist_table

def video_SQL_table(collection_channel):
    df_channel = pd.DataFrame(list(collection_channel.find({},{"_id":0})))
    df_video_table = pd.DataFrame(df_channel['video_data'][0]).transpose()
    video_table = df_video_table[0].apply(pd.Series).transpose()
    video_table['duration'] = video_table['duration'].apply(isodate.parse_duration).apply(lambda x: x.total_seconds()).astype(int)
    video_table['comment_count'] = video_table['comment_count'].astype(int)
    video_table['favorite_count'] = video_table['favorite_count'].astype(int)
    video_table['like_count'] = video_table['like_count'].astype(int)
    video_table['view_count'] = video_table['view_count'].astype(int)
    video_table['published_date'] = pd.to_datetime(video_table['published_date'])
    return video_table

def comment_SQL_table(collection_channel):
    df_channel = pd.DataFrame(list(collection_channel.find({},{"_id":0})))
    comment_df1 = pd.DataFrame(df_channel['comment_table'][0]).transpose()
    comment_df2 = comment_df1[0].apply(pd.Series)
    comment_df3 = comment_df2['comment_id'].explode()
    comment_df4 = comment_df2['video_id'].explode()
    comment_df5 = comment_df2['comment_text'].explode()
    comment_df6 = comment_df2['comment_author'].explode()
    comment_df7 = comment_df2['comment_published_date'].explode()

    comment_df8 = [comment_df3,comment_df4,comment_df5,comment_df6,comment_df7]

    comment_table = pd.concat(comment_df8,axis=1)
    comment_table = comment_table.dropna()
    comment_table = comment_table.reset_index(drop=True)
    return comment_table

channel_table = pd.concat([channel_SQL_table(db[existing_collections[i]]) for i in range (len(existing_collections))]).reset_index(drop=True)
playlist_table = pd.concat([playlist_SQL_table(db[existing_collections[i]]) for i in range (len(existing_collections))]).reset_index(drop=True)
video_table = pd.concat([video_SQL_table(db[existing_collections[i]]) for i in range (len(existing_collections))]).reset_index(drop=True)
comment_table = pd.concat([comment_SQL_table(db[existing_collections[i]]) for i in range (len(existing_collections))]).reset_index(drop=True)
mongo_client.close()

def export_to_Mysql():
    myconnection = pymysql.connect(host='127.0.0.1', user='root', passwd="pwd123", database="youtube_data_harvesting")

    engine = create_engine('mysql+pymysql://root:pwd123@127.0.0.1/youtube_data_harvesting')
    
    channel_table.to_sql("channel_table", con=engine, if_exists='replace', index=False)
    playlist_table.to_sql("playlist_table", con=engine, if_exists='replace', index=False)
    video_table.to_sql("video_table", con=engine, if_exists='replace', index=False)
    comment_table.to_sql("comment_table", con=engine, if_exists='replace', index=False)
    myconnection.close()


tab1, tab2, tab3 = st.tabs(["Channel_List","Export to MySQL","10 SQL Queries"])

with tab1:
   st.header("Tamil cooking Channel List")
   channel_table[['channel_name','channel_views','subcribercount']]
   Export_data = st.button("Export above listed channel data MySQL")

   if Export_data:
      export_to_Mysql()
      st.success('Succesfully stored data in MySql')   


def mysql_query_test(query):
    myconnection = pymysql.connect(host='127.0.0.1', user='root', passwd="pwd123", database="test")
    mycursor = myconnection.cursor()
    sql_query = query 
    mycursor.execute(sql_query)
    result = mycursor.fetchall()
    column = [col[0] for col in mycursor.description]
    df_query= pd.DataFrame(result,columns=column)
    df_query.index = df_query.index + 1 
    df_query.index = df_query.index.rename('SL.No')
    myconnection.close()
    return df_query


with tab2:
   
   
   mongo_client = MongoClient("mongodb://localhost:27017/") 
   db = mongo_client["Youtube_Data"]
   existing_collections = db.list_collection_names()
   st.header("Select a Channel from below Channel list to export data to SQL")
   channel_names = st.selectbox(
    'Channel list',
    (existing_collections))
   
   Export_channel_data = st.button("Export the selected channel to MySQL")

   channel_table_by_selection = channel_SQL_table(db[channel_names])
   playlist_table_by_selection = playlist_SQL_table(db[channel_names])
   video_table_by_selection = video_SQL_table(db[channel_names])
   comment_table_by_selection = comment_SQL_table(db[channel_names])
   mongo_client.close()

   def export_to_Mysql_test():

      myconnection = pymysql.connect(host='127.0.0.1', user='root', passwd="pwd123", database="test")

      engine = create_engine('mysql+pymysql://root:pwd123@127.0.0.1/test')

      channel_table_by_selection.to_sql("channel_table", con=engine, if_exists='replace', index=False)
      playlist_table_by_selection.to_sql("playlist_table", con=engine, if_exists='replace', index=False)
      video_table_by_selection.to_sql("video_table", con=engine, if_exists='replace', index=False)
      comment_table_by_selection.to_sql("comment_table", con=engine, if_exists='replace', index=False)
      myconnection.close()

   if Export_channel_data:
      export_to_Mysql_test()
      st.success('Succesfully stored data in MySql')
      channel_table_query = """ select * from channel_table; """
      playlist_table_query = """ select * from playlist_table;"""
      video_table_query = """ select * from video_table; """
      comment_table_query = """ select * from comment_table; """
      result_channel_tabel = mysql_query_test(channel_table_query)
      result_playlist_tabel = mysql_query_test(playlist_table_query)
      result_video_table = mysql_query_test(video_table_query)
      result_comment_table = mysql_query_test(comment_table_query)
      st.subheader("Channel Table")
      result_channel_tabel
      st.subheader("Playlist Table")
      result_playlist_tabel
      st.subheader("Video Table")
      result_video_table
      st.subheader("Comment table")
      result_comment_table

            
   
with tab3: 
   st.header("10 SQL queries based on Requirment")

   def mysql_query(query):
    myconnection = pymysql.connect(host='127.0.0.1', user='root', passwd="pwd123", database="guvi_youtube_project")
    mycursor = myconnection.cursor()
    sql_query = query 
    mycursor.execute(sql_query)
    result = mycursor.fetchall()
    column = [col[0] for col in mycursor.description]
    df_query= pd.DataFrame(result,columns=column)
    df_query.index = df_query.index + 1 
    df_query.index = df_query.index.rename('SL.No')
    myconnection.close()
    return df_query
   
   query = st.selectbox('10 list of queries',
                        ("01_What are the names of all the videos and their corresponding channels?",
                         "02_Which channels have the most number of videos, and how many videos do they have?",
                         "03_What are the top 10 most viewed videos and their respective channels?",
                         "04_How many comments were made on each video, and what are their corresponding video names?",
                         "05_Which videos have the highest number of likes, and what are their corresponding channel names?",
                         "06_What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                         "07_What is the total number of views for each channel, and what are their corresponding channel names?",
                         "08_What are the names of all the channels that have published videos in the year 2022?",
                         "09_ What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                         "10_Which videos have the highest number of comments, and what are their corresponding channel names?"

                         ))
      
   query_1 = """ select video_table.video_name,channel_table.channel_name
            from video_table
            inner join playlist_table
            on
            video_table.playlist_id = playlist_table.playlist_id
            inner join channel_table
            on
            playlist_table.channel_id = channel_table.channel_id;"""
   
   query_2 = """ select t1.channel_name, count(t3.video_id) as video_count
            from channel_table as t1
            inner join
            playlist_table as t2
            on
            t1.channel_id = t2.channel_id
            inner join	
            video_table as t3
            on
            t2.playlist_id = t3.playlist_id
            group by t1.channel_name
            order by video_count desc
            limit 1;"""
   
   query_3 = """ select t1.channel_name, t3.video_name, t3.view_count
            from channel_table as t1
            inner join
            playlist_table as t2
            on
            t1.channel_id = t2.channel_id
            inner join
            video_table as t3
            on
            t2.playlist_id = t3.playlist_id
            group by t1.channel_name,t3.video_name , t3.view_count
            order by view_count desc
            limit 10;"""
   
   query_4 = """ select t1.video_name, count(t2.comment_id) as comment_count
            from video_table as t1
            inner join
            comment_table as t2
            on
            t1.video_id = t2.video_id
            group by t1.video_name
            order by comment_count desc;"""
   
   query_5 = """ select t1.channel_name, t3.video_name, t3.like_count
            from channel_table as t1
            inner join
            playlist_table as t2
            on
            t1.channel_id = t2.channel_id
            inner join
            video_table as t3
            on
            t2.playlist_id = t3.playlist_id
            group by t1.channel_name, t3.video_name, t3.like_count
            order by like_count desc
            limit 10;"""
   
   query_6 = """ select video_name, like_count
            from video_table
            order by like_count desc;"""
   
   query_7 = """ select channel_name,channel_views 
            from channel_table
            order by channel_views desc;"""
   
   query_8 = """ select t1.channel_name
            from channel_table as t1
            inner join
            playlist_table as t2
            on
            t1.channel_id = t2.channel_id
            inner join
            video_table as t3
            on
            t2.playlist_id = t3.playlist_id
            where year(t3.published_date) = 2022
            group by t1.channel_name
            order by channel_name;
            """
   query_9 = """ select t1.channel_name, avg(t3.duration) as avg_duration_in_seconds
            from channel_table as t1
            inner join
            playlist_table as t2
            on
            t1.channel_id = t2.channel_id
            inner join
            video_table as t3
            on
            t2.playlist_id = t3.playlist_id
            group by t1.channel_name
            order by avg_duration_in_seconds desc;"""
   
   query_10 = """ select t1.channel_name, t3.video_name, t3.comment_count
            from channel_table as t1
            inner join
            playlist_table as t2
            on
            t1.channel_id = t2.channel_id
            inner join
            video_table as t3
            on
            t2.playlist_id = t3.playlist_id
            group by t1.channel_name, t3.video_name, t3.comment_count
            order by comment_count desc
            limit 1;"""
   
   if query == "01_What are the names of all the videos and their corresponding channels?":
    query = query_1
    result_Mysql = mysql_query(query)

   if query == "02_Which channels have the most number of videos, and how many videos do they have?":
    query = query_2
    result_Mysql = mysql_query(query)
   
   if query == "03_What are the top 10 most viewed videos and their respective channels?":
    query = query_3
    result_Mysql = mysql_query(query)

   if query ==  "04_How many comments were made on each video, and what are their corresponding video names?":
    query = query_4
    result_Mysql = mysql_query(query)
   
   if query == "05_Which videos have the highest number of likes, and what are their corresponding channel names?":
    query = query_5
    result_Mysql = mysql_query(query)

   if query == "06_What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    query = query_6
    result_Mysql = mysql_query(query)

   if query == "07_What is the total number of views for each channel, and what are their corresponding channel names?":
    query = query_7
    result_Mysql = mysql_query(query)

   if query == "08_What are the names of all the channels that have published videos in the year 2022?":
    query = query_8
    result_Mysql = mysql_query(query)

   if query == "09_ What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query = query_9
    result_Mysql = mysql_query(query)

   if query == "10_Which videos have the highest number of comments, and what are their corresponding channel names?":
    query = query_10
    result_Mysql = mysql_query(query)

   result_Mysql
   
   
   
  








