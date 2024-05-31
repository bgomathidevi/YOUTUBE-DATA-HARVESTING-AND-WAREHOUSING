
import googleapiclient.discovery
import googleapiclient.errors
import pandas as pd
from googleapiclient.discovery import build
import pymysql
from datetime import datetime
import re
import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns
import altair as alt
import os

# API KEY CONNECTION

def Api_connect():
    Api_id="AIzaSyBJV1ZetcIJl810RlUwqnaJJ4F0-9-jY6U"

    api_service_name="youtube"
    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey=Api_id)

    return youtube
youtube= Api_connect()

# GETTING CHANNEL INFORMATION

def channel_info(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()

    channel_data_list = []
    for item in response['items']:
        data = dict(Channel_Name=item["snippet"]["title"],
                    Channel_Id=item["id"],
                    Subscription_Count=item['statistics']['subscriberCount'],
                    Channel_Views=item["statistics"]["viewCount"],
                    Total_Videos=item["statistics"]["videoCount"],
                    Channel_Description=item["snippet"]["description"],
                    Playlist_Id=item["contentDetails"]["relatedPlaylists"]["uploads"])
        channel_data_list.append(data)
        
    return channel_data_list
   

# GETTING VIDEO IDS

def get_video_ids(channel_id):
    video_ids = []
    response = youtube.channels().list(id=channel_id, part='contentDetails').execute()
    Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1 = youtube.playlistItems().list(
            part='snippet',
            playlistId=Playlist_Id,
            maxResults=50,
            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break

    return video_ids



## GETTING VIDEO INFORMATION

def get_video_info(video_ids):
    video_data = [] 
    for video_id in video_ids:
            request = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=video_id
            )
            response = request.execute()

            for item in response["items"]:
                data = dict(Channel_Name=item['snippet']['channelTitle'],
                            Channel_Id=item['snippet']['channelId'],
                            Video_Id=item['id'],
                            Title=item['snippet']['title'],
                            Tags=item['snippet'].get('tags'),
                            Thumbnail=item['snippet']['thumbnails']['default']['url'],
                            Description=item['snippet'].get('description'),
                            Published_Date=item['snippet']['publishedAt'],
                            Duration=item['contentDetails']['duration'],
                            Views=item['statistics'].get('viewCount'),
                            Like_Count=item['statistics'].get('likeCount'),
                            Comments=item['statistics'].get('commentCount'),
                            Favorite_Count=item['statistics']['favoriteCount'],
                            Definition=item['contentDetails']['definition'],
                            Caption_Status=item['contentDetails']['caption']
                            )
                video_data.append(data)

    return video_data


#### getting comment details
def get_comment_info(video_ids): 
    Comment_data=[]
    
    for video_id in video_ids:
        try:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published_Date=item['snippet']['topLevelComment']['snippet']['publishedAt']
                        )
                Comment_data.append(data)
        except:
            pass
    return Comment_data 

def get_playlist_detail(channel_id):
    next_page_token=None
    Playlist_data=[]
    while True:

        request=youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=50,
            ##page token
            pageToken=next_page_token
            
            
        )
        response=request.execute()


        for item in response['items']:
                data=dict(playlist_id=item['id'],
                        Title=item['snippet']['title'],
                        Channel_id=item['snippet']['channelId'],
                        Channel_Name=item['snippet']['channelTitle'],
                        PublishedAt=item['snippet']['publishedAt'],
                        Video_Count=item.get('itemCount'))
                Playlist_data.append(data)

        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
                    break
    return Playlist_data



# SQL Connection
myconnection = pymysql.connect(host='127.0.0.1', user='root', passwd='595959kgms@karthidevi', db='youtube')

### CHANNEL INFO:

def create_channel_table():
    cursor = myconnection.cursor()
   
    create_query = """
            CREATE TABLE IF NOT EXISTS channel_info (
                Channel_Name VARCHAR(100),
                Channel_Id VARCHAR(100),
                Subscription_Count VARCHAR(100),
                Channel_Views VARCHAR(100),
                Total_Videos VARCHAR(100),
                Channel_Description TEXT,
                Playlist_Id VARCHAR(100)
            )
        """
    cursor.execute(create_query)
    myconnection.commit()


def channels_table(channel_id):
    create_channel_table()
    
    cursor = myconnection.cursor()
    channel_data_list = channel_info(channel_id)
    
    for channel_data in channel_data_list:
        sql = """INSERT INTO channel_info (Channel_Name, Channel_Id, Subscription_Count, Channel_Views, Total_Videos, Channel_Description, Playlist_Id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)"""

        value = (channel_data['Channel_Name'], channel_data['Channel_Id'], channel_data['Subscription_Count'], 
                    channel_data['Channel_Views'], channel_data['Total_Videos'], channel_data['Channel_Description'], 
                    channel_data['Playlist_Id'])
        
        cursor.execute(sql, value)
        myconnection.commit()
    
    channel_df = pd.DataFrame(channel_data_list)
    return channel_df


## PLAYLIST INFO:

def create_playlist_table():
    cursor = myconnection.cursor()
    
    create_query = """
        CREATE TABLE IF NOT EXISTS playlist_info (
            Playlist_id VARCHAR(100),
            Title VARCHAR(100),
            Channel_id VARCHAR(100),
            Channel_Name VARCHAR(100),
            PublishedAt DATETIME,
            Video_Count VARCHAR(100)
        )
    """
    cursor.execute(create_query)
    myconnection.commit()
    
    

def playlists_table(channel_id):
    from datetime import datetime
    create_playlist_table()
    cursor = myconnection.cursor()
    channel_data = get_playlist_detail(channel_id)
    playlist_data_list = [] 

    for playlist in channel_data:
        sql = """INSERT INTO playlist_info (Playlist_id, Title, Channel_id, Channel_Name, PublishedAt, Video_Count)
                VALUES (%s, %s, %s, %s, %s, %s)"""
        
        published_at = datetime.strptime(playlist['PublishedAt'], "%Y-%m-%dT%H:%M:%SZ")
        
        values = (playlist['playlist_id'], playlist['Title'], playlist['Channel_id'], playlist['Channel_Name'],
                published_at, playlist['Video_Count'])
       
        cursor.execute(sql, values)
        myconnection.commit()
       
        playlist_data_list.append(playlist)

    playlist_df = pd.DataFrame(playlist_data_list)
    return playlist_df


## VIDEO INFOR:

def create_video_table():
    cursor = myconnection.cursor()
    create_query = """
        CREATE TABLE IF NOT EXISTS video_info (
            Channel_Name VARCHAR(100),
            Channel_Id VARCHAR(100),
            Video_Id VARCHAR(100),
            Title TEXT,
            Tags TEXT,
            Thumbnail VARCHAR(100),
            Description TEXT,
            Published_Date DATETIME,
            Duration TIME,
            Views VARCHAR(100),
            Like_Count VARCHAR(100),
            Comments TEXT,
            Favorite_Count VARCHAR(100),
            Definition TEXT,
            Caption_Status TEXT
        )
    """
    cursor.execute(create_query)
    myconnection.commit()

def videos_table(channel_id):
    cursor = myconnection.cursor()
    create_video_table()
    video_ids = get_video_ids(channel_id)
    video_data = get_video_info(video_ids)
    video_data_list = [] 

    for video in video_data:
        sql = """INSERT INTO video_info (Channel_Name, Channel_Id, Video_Id, Title, Tags, Thumbnail, Description,
                                        Published_Date, Duration, Views, Like_Count, Comments, Favorite_Count,
                                        Definition, Caption_Status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        
        published_date = datetime.strptime(video['Published_Date'], "%Y-%m-%dT%H:%M:%SZ")

        duration = video['Duration']
        duration_formatted=duration.replace('PT', ' ').replace('H', ':').replace('M', ':').split('S')[0].strip()
        parts = duration_formatted.split(':')
        duration_formatted = ':'.join([part.zfill(2) for part in parts])

        tags_string = ', '.join(video['Tags']) if video['Tags'] else None
        
        values = (video['Channel_Name'], video['Channel_Id'], video['Video_Id'], video['Title'], tags_string,
                video['Thumbnail'], video['Description'], published_date, duration_formatted, video['Views'],
                video['Like_Count'], video['Comments'], video['Favorite_Count'], video['Definition'],
                video['Caption_Status'])
        cursor.execute(sql, values)
        myconnection.commit()

        video_data_list.append(video)

    video_df = pd.DataFrame(video_data_list)
    return video_df


## COMMENT INFO:

def create_comment_table():
    cursor = myconnection.cursor()
    
    create_query = """
        CREATE TABLE IF NOT EXISTS comment_info (
            Comment_Id VARCHAR(100),
            Video_Id VARCHAR(100),
            Comment_Text TEXT,
            Comment_Author VARCHAR(100),
            Comment_Published_Date DATETIME
        )
    """
    cursor.execute(create_query)
    myconnection.commit()
    

    
    
def comments_table(channel_id):
    create_comment_table()
    cursor = myconnection.cursor()
    video_ids = get_video_ids(channel_id)
    channel_data = get_comment_info(video_ids)
    comment_data_list = []

    for comment in channel_data:
        
            sql = """INSERT INTO comment_info (Comment_Id, Video_Id, Comment_Text, Comment_Author, Comment_Published_Date)
                    VALUES (%s, %s, %s, %s, %s)"""
            comment_published_date = datetime.strptime(comment['Comment_Published_Date'], "%Y-%m-%dT%H:%M:%SZ")
            values = (comment['Comment_Id'], comment['video_Id'], comment['Comment_Text'], comment['Comment_Author'],
                    comment_published_date)
            cursor.execute(sql, values)
            myconnection.commit()

            
            comment_data_list.append(comment)
        
    comment_df = pd.DataFrame(comment_data_list)
    return comment_df

## QUERY
def execute_query(query):
    cursor = myconnection.cursor()
    cursor.execute(query)
    columns = [col[0] for col in cursor.description]
    results = cursor.fetchall()
    df = pd.DataFrame(results, columns=columns)
    return df

## STREAMLIT 

def my_function():

    page_bg_img = '''
    <style>
    .stApp {
        background-color: #FFD8DC; 
    }
    </style>
    '''

    st.set_page_config(page_title= "Youtube Data Extraction and Warehousing",
                    layout= "wide",
                    initial_sidebar_state= "expanded",)
    st.markdown("<h1 style='text-align: center; color: red;'>YOUTUBE DATA HARVESTING AND WAREHOUSING</h1>", unsafe_allow_html=True)
    st.markdown(page_bg_img,unsafe_allow_html=True)
    st.divider()
    
    SELECT = st.sidebar.radio("SELECT OPTION", ["About", "Collect and Store", "Queries"])
    st.sidebar.header("SKILLS TAKE AWAY")
    st.sidebar.markdown("Python scripting")
    st.sidebar.markdown("Data Collection")
    st.sidebar.markdown("Streamlit")
    st.sidebar.markdown("API integration")
    st.sidebar.markdown("Data Management using SQL")

    folder_path = "G:/PROJECT/project_1-YOUTUBE"

    if SELECT == "About":
       
        col1,col2 = st.columns([2,2],gap="large")
        with col1:
            
            st.header("OBJECTIVE")
            st.write("The YouTube Data Harvesting and Warehousing application serves as a valuable tool for extracting, storing, and analyzing YouTube data, offering insights that drive informed decision-making and optimization strategies for content creators, marketers, and analysts.")
            st.write("It allows us to collect data from a YouTube channel, store it in a database, and perform queries on the collected data.")
            st.write("---")
        with col2:
            st.write("---")
            image_path = os.path.join(folder_path, "1.png")
            # Display the image
            st.image(image_path, width=500)
            st.write("---")
            

    elif SELECT == "Collect and Store":
        st.title("Collect and Store Data")
        channel_id = st.text_input("ENTER THE CHANNEL ID")
        col1,col2 = st.columns([2,2],gap="large")
        with col1:
            if st.button("COLLECT AND STORE DATA"):
                channel_df = None
                playlist_df = None
                video_df = None
                comment_df = None

                # MySQL connection
                myconnection = pymysql.connect(host='127.0.0.1', user='root', passwd='595959kgms@karthidevi', db='youtube')
                cursor = myconnection.cursor()

                # Create tables
                create_channel_table()
                create_playlist_table()
                create_video_table()
                create_comment_table()

                # Validate channel_id
                if not channel_id:
                    st.warning("Please enter a valid Channel ID.")
                    return

                # Checking channel ID
                cursor.execute("SELECT Channel_Id FROM channel_info WHERE Channel_Id = %s", (channel_id,))
                existing_channel = cursor.fetchone()

                if existing_channel:
                    st.warning("Channel ID already exists in the database.")
                else:
                    # Inserting data
                    channel_df = channels_table(channel_id)
                    playlist_df = playlists_table(channel_id)
                    video_df = videos_table(channel_id)
                    comment_df = comments_table(channel_id)

                    st.success("Data collection and storage completed successfully.")
                col1,col2,col3,col4 = st.columns([2.5,2.5,2.5,2.5],gap="large")
                with col1:
                    if channel_df is not None:
                        st.write("Channel DataFrame:", channel_df)
                with col2:
                    if playlist_df is not None:
                        st.write("Playlist DataFrame:", playlist_df)
                with col3:
                    if video_df is not None:
                        st.write("Video DataFrame:", video_df)
                with col4:
                    if comment_df is not None:
                        st.write("Comment DataFrame:", comment_df)
           

    elif SELECT == "Queries":
        st.title("Queries")
        question = st.selectbox("SELECT THE QUESTIONS", (
        "1. Names of all videos and their channels",
        "2. Channels with the highest number of videos",
        "3. The top 10 most viewed videos and their channels",
        "4. Number of comments on each video and their video name",
        "5. Videos have the highest number of likes and their channel name",
        "6. The total number of likes for each videos and their video name",
        "7. The total number of views for each channel and their channel name",
        "8. Names of the channels that published in the year 2022",
        "9. The average duration of all videos in each channel and their channel name",
        "10. Videos have the highest number of comments and their channel name"
    ))

        if st.button("Get Answer"):
        
                if question == "1. Names of all videos and their channels":
                    col1,col2 = st.columns([2,2],gap="large")
                    with col1:
                        query = "SELECT Title, Channel_Name FROM video_info"
                        df = execute_query(query)
                        st.write(df)
                    with col2:
                        chart = alt.Chart(df).mark_circle(size=60).encode(
                            x='Title',
                            y='Channel_Name',
                            tooltip=['Title', 'Channel_Name']
                        ).properties(width=800, height=400).interactive()
                        
                        st.altair_chart(chart, use_container_width=True)

                    
                elif question == "2. Channels with the highest number of videos":
                    col1,col2 = st.columns([2,2],gap="large")
                    with col1:
                        query = """
                            SELECT Channel_Name, COUNT(Video_Id) AS Video_Count
                            FROM video_info
                            GROUP BY Channel_Name
                            ORDER BY Video_Count DESC;

                        """
                        df = execute_query(query)
                        st.write(df)
                    with col2:
                        chart = alt.Chart(df).mark_point().encode(
                            x='Channel_Name:N',
                            y='Video_Count:Q',
                            tooltip=['Channel_Name', 'Video_Count']
                        ).properties(width=400, height=400).interactive()
                        
                        st.altair_chart(chart, use_container_width=True)


                elif question == "3. The top 10 most viewed videos and their channels":
                    col1,col2 = st.columns([2,2],gap="large")
                    with col1:
                        query = """
                            SELECT Title AS Video_Title, Channel_Name, Views
                            FROM video_info
                            ORDER BY Views DESC
                            LIMIT 10;
                        """
                        df = execute_query(query)
                        st.write(df)
                    with col2:
                        line_chart = alt.Chart(df).mark_line().encode(
                            x='Video_Title',
                            y='Views',
                            color='Channel_Name',
                            tooltip=['Video_Title', 'Channel_Name', 'Views']
                        ).properties(
                            width=800,
                            height=400
                        ).interactive()

                        st.altair_chart(line_chart, use_container_width=True)

            
                elif question == "4. Number of comments on each video and their video name":
                    col1,col2 = st.columns([2,2],gap="large")
                    with col1:
                        query = """
                            SELECT Title AS Video_Name, COUNT(*) AS Comment_Count
                            FROM comment_info
                            JOIN video_info ON comment_info.Video_Id = video_info.Video_Id
                            GROUP BY video_info.Video_Id, video_info.Title
                        """
                        df = execute_query(query)
                        st.write(df)
                    with col2:
                        bar_chart = alt.Chart(df).mark_bar().encode(
                            x='Video_Name',
                            y='Comment_Count',
                            tooltip=['Video_Name', 'Comment_Count']
                        ).properties(width=800, height=400).interactive()
                        
                        st.altair_chart(bar_chart, use_container_width=True)




                elif question == "5. Videos have the highest number of likes and their channel name":
                    col1,col2 = st.columns([3,7],gap="large")
                    with col1:
                        query = """
                            SELECT v.Title AS Video_Title, v.Channel_Name, v.Like_Count
                            FROM video_info v
                            WHERE v.Like_Count = (
                                SELECT MAX(Like_Count)
                                FROM video_info 
                            )
                        """
                        df = execute_query(query)
                        st.write(df)
                    with col2:
                        bar_chart = alt.Chart(df).mark_bar().encode(
                            x='Video_Title',
                            y='Like_Count',
                            tooltip=['Video_Title', 'Like_Count']
                        ).properties(width=800, height=400).interactive()
                        
                        st.altair_chart(bar_chart, use_container_width=True)



                elif question == "6. The total number of likes for each videos and their video name":
                    col1,col2 = st.columns([3,7],gap="large")
                    with col1:
                        query = """
                            SELECT Title AS Video_Name, SUM(Like_Count) AS Total_Likes
                            FROM video_info
                            GROUP BY Video_Id, Title
                        """
                        df = execute_query(query)
                        st.write(df)
                    with col2:

                        bar_chart = alt.Chart(df).mark_bar().encode(
                            x='Video_Name',
                            y='Total_Likes',
                            tooltip=['Video_Name', 'Total_Likes']
                        ).properties(width=800,height=400).interactive()
                        
                        st.altair_chart(bar_chart, use_container_width=True)



                elif question == "7. The total number of views for each channel and their channel name":
                    col1,col2 = st.columns([2,2],gap="large")
                    with col1:
                        query = """
                            SELECT Channel_Name, SUM(Views) AS Total_Views
                            FROM video_info
                            GROUP BY Channel_Id, Channel_Name limit 5
                        """
                        df = execute_query(query)
                        st.write(df)
                    with col2:

                        bar_chart = alt.Chart(df).mark_bar().encode(
                            x='Channel_Name',
                            y='Total_Views',
                            tooltip=['Channel_Name', 'Total_Views']
                        ).properties(width=800, height=400).interactive()

                        st.altair_chart(bar_chart, use_container_width=True)



                elif question == "8. Names of the channels that published in the year 2022":
                    col1,col2 = st.columns([2,2],gap="large")
                    with col1:
                        query = "SELECT DISTINCT Channel_Name FROM playlist_info WHERE YEAR(PublishedAt) = 2022"
                        df = execute_query(query)
                        st.write(df)

                    with col2:
                        bar_chart = alt.Chart(df).mark_bar().encode(
                            x='Channel_Name',
                            y='count()',
                            tooltip=['Channel_Name']
                        ).properties(width=800, height=400).interactive()
                        
                        st.altair_chart(bar_chart, use_container_width=True)
                    


                elif question == "9. The average duration of all videos in each channel and their channel name":
                    col1,col2 = st.columns([2,2],gap="large")
                    with col1:
                        query = """
                            SELECT Channel_Name, 
                                            SEC_TO_TIME(AVG(TIME_TO_SEC(Duration))) AS Average_Duration
                            FROM video_info
                            GROUP BY Channel_Name;

                        """
                        df = execute_query(query)
                        st.write(df)

                    with col2:
                        bar_chart = alt.Chart(df).mark_bar().encode(
                            x='Channel_Name',
                            y='Average_Duration',
                            tooltip=['Channel_Name', 'Average_Duration']
                        ).properties(width=800, height=400).interactive()
                        
                        st.altair_chart(bar_chart, use_container_width=True)
                    



                elif question == "10. Videos have the highest number of comments and their channel name":
                    col1,col2 = st.columns([2,2],gap="large") 
                    with col1:
                        query = "SELECT Title, Comments FROM video_info ORDER BY Comments DESC LIMIT 1"
                        df = execute_query(query)
                        st.write(df)
                        
                    
                    
                
my_function()






   