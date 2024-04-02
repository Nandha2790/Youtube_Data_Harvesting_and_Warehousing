import streamlit as st
import googleapiclient.discovery
import json
import pandas as pd
from pymongo import MongoClient
from googleapiclient.errors import HttpError
import pymysql
from sqlalchemy import create_engine
import isodate
import plotly.express as px

st.markdown("## :red[YouTube] Data Harvesting and Warehousing" ) #title of the project

with st.sidebar:
        
    st.header("Enter the below credentials")   
        
    channel_id = st.text_input("Channel_id:",placeholder="Enter your channel id") #for user to enter the channel id  

    api = st.text_input("API_key:",placeholder="Enter your API key") # for user to enter the channel id

    if channel_id and api:
        st.success('Thanks for your input, Please select the below option to Import the data in MongoDB')

    import_data_to_mongodb = st.button ("Import Data to MongoDB") # button for import the data into MongoDB
    
    if import_data_to_mongodb:   
        
        ch_id= channel_id
        api_key = api

        api_service_name = "youtube"
        api_version = "v3"

        youtube = googleapiclient.discovery.build(
            api_service_name, api_version, developerKey=api_key)



        request_channel = youtube.channels().list(
                    part="snippet,contentDetails,statistics,status",
                    id=channel_id
        )
        response_channel = request_channel.execute()
        channel_name = response_channel['items'][0]['snippet']['title']
        channel_data = {
            "channel_id" : response_channel['items'][0]['id'],
            "channel_name" : channel_name,
            "channel_description" : response_channel['items'][0]['snippet']['description'],
            "channel_type" : "cooking" if "cooking" or "cook" in response_channel["items"][0]["snippet"]["description"] else None,
            "channel_views" : int(response_channel['items'][0]['statistics']['viewCount']),
            "subcribercount" : int(response_channel['items'][0]['statistics']['subscriberCount']),
            "total_video_count" : int(response_channel['items'][0]['statistics']['videoCount']),
            "channel_status" : response_channel['items'][0]['status']['privacyStatus']
            } 

        channel_table = {channel_name: channel_data}
        
        def pl_table(channel_id): # function to get playlist table
            pl_data = []   
            
            request_playlist_table = youtube.playlists().list(
                    part="snippet,contentDetails",
                    channelId=channel_id,
                    maxResults=50
            )
            response_playlist_table = request_playlist_table.execute()
            channel_name = response_playlist_table['items'][0]['snippet']['channelTitle']
            for i in range(len(response_playlist_table['items'])):
                playlist_data = {
                    'playlist_id': response_playlist_table['items'][i]['id'],
                    'channel_id' : response_playlist_table['items'][i]['snippet']['channelId'],
                    'playlist_name': response_playlist_table['items'][i]['snippet']['title']             
                    
                }
                pl_data.append({"playlist_id"+"_"+str(i):playlist_data})
            
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
                        playlist_data = {
                            'playlist_id': response_playlist_table['items'][i]['id'],
                            'channel_id' : response_playlist_table['items'][i]['snippet']['channelId'],
                            'playlist_name': response_playlist_table['items'][i]['snippet']['title']             
                            
                        }
                        pl_data.append({"playlist_id"+"_"+str(i):playlist_data})       
                    
                    nextpagetoken= response_playlist_table.get("nextPageToken")
                    
                return {channel_name:pl_data}
            

        def video_table(playlist_id):
            video = []
            request_video_id = youtube.playlistItems().list(
                part="snippet,contentDetails",
                maxResults=50,
                playlistId=playlist_id
            )
            response_video_id = request_video_id.execute()
            for i in range(len(response_video_id['items'])):
                video_id = response_video_id['items'][i]['contentDetails']['videoId']
                request_video_list = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=video_id
                )
                response_video_list = request_video_list.execute()
                
                if response_video_list ["pageInfo"]['totalResults'] != 0:
                    
                    video_data = {        
                    
                        'video_id': response_video_list['items'][0]['id'],
                        "playlist_id":response_video_id['items'][0]['snippet']['playlistId'],
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
                    video.append({"video_id"+"_"+str(i+1):video_data})
            # Check if the playlist is empty
            if not response_video_id['items']:
                pass
            nextpagetoken= response_video_id.get("nextPageToken")
            next_page = True
            while next_page:
                if nextpagetoken is None:
                    next_page = False
                else:
                    
                    request_video_id = youtube.playlistItems().list(                        
                        part="snippet,contentDetails",
                        maxResults=50,
                        playlistId=playlist_id
                    )
                    response_video_id = request_video_id.execute()
                    for i in range(len(response_video_id['items'])):
                        video_id = response_video_id['items'][i]['contentDetails']['videoId']
                        request_video_list = youtube.videos().list(
                        part="snippet,contentDetails,statistics",
                        id=video_id
                        )
                        response_video_list = request_video_list.execute()
                        
                        if response_video_list ["pageInfo"]['totalResults'] != 0:
                            
                            video_data = {        
                            
                                'video_id': response_video_list['items'][0]['id'],
                                "playlist_id":response_video_id['items'][0]['snippet']['playlistId'],
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
                            video.append({"video_id"+"_"+str(i+1):video_data})
                    nextpagetoken= response_video_id.get("nextPageToken")
                return video              
            

        def comment_thread(video_id):
            comment_data = []

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
                            comment = {
                                'comment_id': response_comment['items'][i]['id'],
                                'video_id': response_comment['items'][i]['snippet']['videoId'],
                                "comment_text": response_comment['items'][i]['snippet']['topLevelComment']['snippet']['textDisplay'],
                                'comment_author': response_comment['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                'comment_published_date': response_comment['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt']
                            }
                            comment_data.append({"comment_id"+"_"+str(i+1):comment})
                        if nextpagetoken is None:
                                break
            except HttpError as e:
                    pass  # Skip video if comments are disabled

            return comment_data 

        playlist_table = pl_table(channel_id)
        df_playlist = pd.concat([pd.DataFrame(playlist_table[channel_name][i]).transpose() for i in range(len(playlist_table[channel_name]))])
        playlist_ids = list(df_playlist['playlist_id'])
        video = [video_table(i) for i in playlist_ids]
        df_video = pd.concat([pd.DataFrame(video[i][j]).transpose() for i in range(len(video)) for j in range(len(video[i]))],axis=0)
        video_ids = list(df_video['video_id'])
        comment = [comment_thread(i) for i in video_ids]

        video_data = {channel_name:video}
        comment_data = {channel_name:comment}

        # Setting up MongoDB connection
        mongo_client = MongoClient("mongodb://localhost:27017/") 

        db_name = "youtube"

        collection_name_channel = "channel_data"
        collection_name_playlist = "playlist_data"
        collection_name_video = "video_data"
        collection_name_comment = "comment_data"

        db = mongo_client[db_name]
        collection_channel = db[collection_name_channel]
        collection_playlist = db[collection_name_playlist]
        collection_video = db[collection_name_video]
        collection_comment = db[collection_name_comment]

        # To store channel data
        if collection_name_channel not in db.list_collection_names():
            collection_channel.insert_one(channel_table)

        else:
            collection_channel.update_many({},{"$set":channel_table})

        # To store playlist data
        if collection_name_playlist not in db.list_collection_names():
            collection_playlist.insert_one(playlist_table)

        else:
            collection_playlist.update_many({},{"$set":playlist_table})
            
        # To store video data
        if collection_name_video not in db.list_collection_names():
            collection_video.insert_one(video_data)

        else:
            collection_video.update_many({},{"$set":video_data})
            
        # To store video data
        if collection_name_comment not in db.list_collection_names():
            collection_comment.insert_one(comment_data)

        else:
            collection_comment.update_many({},{"$set":comment_data})

        # Close the MongoDB connection
        mongo_client.close()
        
        if channel_id and api:
                st.success('Succesfully stored data in MongoDB')
        else:
                if not channel_id:
                    st.warning("Please enter channel id")
                if not api:
                    st.warning("Please enter your api key")
                

tab1, tab2, tab3 = st.tabs(["The output","Export to MySQL","10 SQL Queries"])


tab1.subheader("Output:")

with tab1:        
                    
    if import_data_to_mongodb:
        st.subheader(f"Channel Name: '{channel_name}' Data has been extracted from Youtube using Google API")
        ch_data = {channel_name:channel_data,"playlist_details":playlist_table,"video_details":video_data,"comment_details":comment_data}
        st.write(ch_data)
        

# Set up MongoDB connection
mongo_client = MongoClient("mongodb://localhost:27017/") 
db = mongo_client["youtube"]
existing_collections = db.list_collection_names()

# channel table from "channel_data" collection
channel_table = pd.DataFrame(list(db['channel_data'].find({},{"_id":0}))[0]).transpose() 

# Playlist_table from "playlist_data" collection
df_playlist= pd.DataFrame(list(db['playlist_data'].find({},{"_id":0}))) # to get channel names

pl_columns = list(df_playlist.columns) #channel names

pl_data = list(db['playlist_data'].find({},{"_id":0})) # converting the "playlist data" mongodb collection to a list

playlist_data = [pl_data[i][j] for i in range(len(pl_data)) for j in pl_columns] # converting the playlist data for all channels into a single list

playlist_data1 = [playlist_data[i][j] for i in range(len(playlist_data)) for j in range(len(playlist_data[i]))] #merging the nested list into a single list, it will have all the playlist data in a single list

playlist_table = pd.concat([pd.DataFrame(playlist_data1[i]).transpose() for i in range(len(playlist_data1)) ]) # using concordinate function, converting tha data into playlist table dataframe

# video table from "video_data" collection
df_video = pd.DataFrame(list(db['video_data'].find({},{"_id":0}))) # to get channel names

vi_columns = list(df_video.columns) #channel names

vi_data = list(db['video_data'].find({},{"_id":0})) # converting the "video_data" mongodb collection to a list

video_data = [vi_data[i][j] for i in range(len(vi_data)) for j in vi_columns] # converting the "video data " for all channels into a single list

video_data1 = [video_data[i][j] for i in range(len(video_data)) for j in range(len(video_data[i]))] #merging the nested list into a single list, it will have all the "video data" in a single list

video_table = pd.concat([pd.DataFrame(video_data1[i][j]).transpose() for i in range(len(video_data1)) for j in range(len(video_data1[i]))]) # using concordinate function, converting tha data into video table dataframe

video_table['duration'] = video_table['duration'].apply(isodate.parse_duration).apply(lambda x: x.total_seconds()).astype(int)

video_table['comment_count'] = video_table['comment_count'].astype(int)

video_table['favorite_count'] = video_table['favorite_count'].astype(int)

video_table['like_count'] = video_table['like_count'].astype(int)

video_table['view_count'] = video_table['view_count'].astype(int)

video_table['published_date'] = pd.to_datetime(video_table['published_date'])

# comment table from "comment_data" collection
df_comment = pd.DataFrame(list(db['comment_data'].find({},{"_id":0}))) # to get channel names

com_columns = list(df_video.columns) #channel names in "channel data" collection

com_data = list(db['comment_data'].find({},{"_id":0})) # converting the "comment_data" mongodb collection to a list

comment_data = [com_data[i][j] for i in range(len(com_data)) for j in com_columns] # converting the "comment data " for all channels into a single list

comment_data1 = [comment_data[i][j] for i in range(len(comment_data)) for j in range(len(comment_data[i]))] #merging the nested list into a single list, it will have all the "comment data" in a single list

comment_table = pd.concat([pd.DataFrame(comment_data1[i][j]).transpose() for i in range(len(comment_data1)) for j in range(len(comment_data1[i]))]) # using concordinate function, converting tha data into comment table dataframe

mongo_client.close()


def export_to_Mysql():
     
    myconnection = pymysql.connect(host='127.0.0.1', user='root', passwd="pwd123", database="youtube_data_harvesting")

    engine = create_engine('mysql+pymysql://root:pwd123@127.0.0.1/youtube_data_harvesting')
    
    channel_table.to_sql("channel_table", con=engine, if_exists='replace', index=False)
    playlist_table.to_sql("playlist_table", con=engine, if_exists='replace', index=False)
    video_table.to_sql("video_table", con=engine, if_exists='replace', index=False)
    comment_table.to_sql("comment_table", con=engine, if_exists='replace', index=False)
    myconnection.close()
    
engine = create_engine('mysql+pymysql://root:pwd123@127.0.0.1/youtube_data_harvesting')

channel_sql_df = pd.read_sql_table("channel_table",engine)

channel_sql_df.index =channel_sql_df.index+1
channel_sql_df.index = channel_sql_df.index.rename("Sl.No")

tab2.subheader("Channel List:")

tab2.dataframe(channel_sql_df[["channel_name","channel_views","subcribercount"]],use_container_width=True)

fig = px.bar(
    channel_sql_df,
    x = 'channel_name',
    y = 'subcribercount',
    color='channel_views',
    text_auto='.2s',
    color_continuous_scale="Portland",
    title="Tamil cooking Channels"
    )

fig.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)

tab2.plotly_chart(fig,use_container_width=True)

export_data = tab2.button("Export data to MySQL")

with tab2:
    if export_data:
        export_to_Mysql()
        st.success("sucessfully added in 'youtube_data_harvesting' database")

engine = create_engine('mysql+pymysql://root:pwd123@127.0.0.1/youtube_data_harvesting') 

channel_name_mysql = tab2.selectbox("Channel_name",com_columns)

channel_table_query = f"select * from channel_table where channel_name = '{channel_name_mysql}';"

channel_table_mysql = pd.read_sql_query(channel_table_query,engine)

tab2.subheader("Channel Table:")

tab2.dataframe(channel_table_mysql,use_container_width=True)

playlist_table_query = f"select * from playlist_table where channel_id = '{list(channel_table_mysql['channel_id'])[0]}';"

playlist_table_mysql = pd.read_sql_query(playlist_table_query,engine)

tab2.subheader("Playlist Table:")

tab2.dataframe(playlist_table_mysql,use_container_width=True)

video_table_query = f""" select t3.* from channel_table as t1
                        inner join playlist_table as t2
                        on t1.channel_id = t2.channel_id
                        inner join video_table as t3
                        on t2.playlist_id = t3.playlist_id
                        where t1.channel_id = '{list(channel_table_mysql['channel_id'])[0]}';
                        """
                        
video_table_mysql = pd.read_sql_query(video_table_query,engine)

tab2.subheader("Video Table:")

tab2.dataframe(video_table_mysql,use_container_width=True)

comment_table_query = f""" select t4.* from channel_table as t1
                        inner join
                        playlist_table as t2
                        on t1.channel_id = t2.channel_id
                        inner join
                        video_table as t3
                        on t2.playlist_id = t3.playlist_id
                        inner join
                        comment_table as t4
                        on t3.video_id = t4.video_id                        
                        where t1.channel_id = '{list(channel_table_mysql['channel_id'])[0]}';
                        """
                        
comment_table_mysql = pd.read_sql_query(comment_table_query,engine)

tab2.subheader("comment Table:")

tab2.dataframe(comment_table_mysql,use_container_width=True)

username = tab2.text_input("Username",placeholder="Enter your user name")

password = tab2.text_input("password:",placeholder="Enter your password",type="password")

hostname = tab2.text_input("Hostname",placeholder="Enter the hostname")

db_name = tab2.text_input("Database Name",placeholder="Enter the database name")

export_mysql_channel = tab2.button("Export selected channel to Mysql")

if export_mysql_channel:
        
    engine_test = create_engine(f'mysql+pymysql://{username}:{password}@{hostname}/{db_name}')
    
    channel_table_mysql.to_sql("channel_table", con=engine_test, if_exists='replace', index=False)
    
    playlist_table_mysql.to_sql("playlist_table", con=engine_test, if_exists='replace', index=False)
    
    video_table_mysql.to_sql("video_table", con=engine_test, if_exists='replace', index=False)
    
    comment_table_mysql.to_sql("comment_table", con=engine_test, if_exists='replace', index=False)
    
    st.success(f"Succesfully stored data in {db_name} database ")
    
query = tab3.selectbox('10 list of queries',
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
    
query_1 = """ select distinct video_table.video_name,channel_table.channel_name
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

query_6 = """ select distinct video_name, like_count
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
    tab3.dataframe(pd.read_sql_query(query,engine),use_container_width=True)

if query == "02_Which channels have the most number of videos, and how many videos do they have?":
    query = query_2
    tab3.dataframe(pd.read_sql_query(query,engine),use_container_width=True)

if query == "03_What are the top 10 most viewed videos and their respective channels?":
    query = query_3
    tab3.dataframe(pd.read_sql_query(query,engine),use_container_width=True)

if query ==  "04_How many comments were made on each video, and what are their corresponding video names?":
    query = query_4
    tab3.dataframe(pd.read_sql_query(query,engine),use_container_width=True)

if query == "05_Which videos have the highest number of likes, and what are their corresponding channel names?":
    query = query_5
    tab3.dataframe(pd.read_sql_query(query,engine),use_container_width=True)

if query == "06_What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    query = query_6
    tab3.dataframe(pd.read_sql_query(query,engine),use_container_width=True)

if query == "07_What is the total number of views for each channel, and what are their corresponding channel names?":
    query = query_7
    tab3.dataframe(pd.read_sql_query(query,engine),use_container_width=True)

if query == "08_What are the names of all the channels that have published videos in the year 2022?":
    query = query_8
    tab3.dataframe(pd.read_sql_query(query,engine),use_container_width=True)

if query == "09_ What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query = query_9
    tab3.dataframe(pd.read_sql_query(query,engine),use_container_width=True)

if query == "10_Which videos have the highest number of comments, and what are their corresponding channel names?":
    query = query_10
    tab3.dataframe(pd.read_sql_query(query,engine),use_container_width=True)
