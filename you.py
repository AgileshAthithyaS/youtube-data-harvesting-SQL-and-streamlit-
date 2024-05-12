import pandas as pd
import streamlit as st
import googleapiclient.discovery
import re
from datetime import timedelta
from datetime import datetime

api_service_name = "youtube"
api_version = "v3"

api_key='AIzaSyCM3irGC8-VHdDdcyHSggYudj53RQPGPwg'
youtube = googleapiclient.discovery.build(
        api_service_name, api_version,developerKey=api_key)


#function for getting channel details
def get_channel_info(channel_id):
    
            request=youtube.channels().list(
                            part="snippet,ContentDetails,statistics",
                            id=channel_id
            )
            response=request.execute()
            
            
            
            for i in response['items']:
                data=dict(Channel_Name=i["snippet"]["title"],
                        Channel_Id=i["id"],
                        Subscribers=i['statistics']['subscriberCount'],
                        Views=i["statistics"]["viewCount"],
                        Total_Videos=i["statistics"]["videoCount"],
                        Channel_Description=i["snippet"]["description"],
                        Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
            return data

#function for getting video_ids from channel ID
def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
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

#converting duration string  into seconds for mysql database
def string_duration_seconds(temporary_duration):
    hours_pattern = re.compile(r'(\d+)H')
    minutes_pattern = re.compile(r'(\d+)M')
    seconds_pattern = re.compile(r'(\d+)S')
    total_seconds = 0
    hours = hours_pattern.search(temporary_duration)
    minutes = minutes_pattern.search(temporary_duration)
    seconds = seconds_pattern.search(temporary_duration)

    hours = int(hours.group(1)) if hours else 0
    minutes = int(minutes.group(1)) if minutes else 0
    seconds = int(seconds.group(1)) if seconds else 0

    video_seconds = timedelta(
                hours=hours,
                minutes=minutes,
                seconds=seconds
            ).total_seconds()

    total_seconds += video_seconds
    return total_seconds


#getting video details from video IDS
def extract_video_info(video_Ids):
    
    video_data=[]
    for video_id in video_Ids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        response=request.execute()

        for item in response["items"]:
            published_date = datetime.strptime(item['snippet']['publishedAt'], '%Y-%m-%dT%H:%M:%SZ')
            formatted_published = published_date.strftime('%Y-%m-%d %H:%M:%S')
            temp_duration=str(item['contentDetails']['duration'])
            total_duration=string_duration_seconds(temp_duration)

            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Title=item['snippet']['title'],
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    formatted_published=published_date.strftime('%Y-%m-%d %H:%M:%S'),
                    video_duration_seconds = (total_duration),
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Favorite_Count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_Status=item['contentDetails']['caption']
                    )
            video_data.append(data)
    return video_data

#getting comment info  from video IDS
def get_comment_info(video_Ids):
    Comment_data=[]
    try:
        for video_id in video_Ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                published_date = datetime.strptime(item['snippet']['topLevelComment']['snippet']['publishedAt'], '%Y-%m-%dT%H:%M:%SZ')
                formatted_published = published_date.strftime('%Y-%m-%d %H:%M:%S')
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        formatted_published = published_date.strftime('%Y-%m-%d %H:%M:%S'))

                Comment_data.append(data)

    except:
        pass
    return Comment_data

#####                                       End of functions                                         ######


#####                                                    MAIN CODE BLOCK                                             #####


#### MYSQL connection

import mysql.connector 

connection=mysql.connector.connect(
host='localhost',
user='root',
password='Agilesh@8418',
database='youtube_project'
)
cursor=connection.cursor(buffered = True)  

#### creation of DATABASE as youtube_project

cursor.execute (''' CREATE DATABASE IF NOT EXISTS youtube_project;''')


cursor.execute('use youtube_project')

### Creation of TABLE (note: columns of table should be based on extraction of data)

cursor.execute('''create table IF NOT EXISTS channels (Channel_Name VARCHAR(100), Channel_ID VARCHAR(50) PRIMARY KEY , Subscribers INT(50), Views INT(50),Total_Videos INT(255), Channel_Description VARCHAR(10000),Playlist_Id VARCHAR(255))''')
cursor.execute('''create table  IF NOT EXISTS videos (Channel_Name VARCHAR(255), Channel_Id VARCHAR(100), Video_Id VARCHAR(255) , Title VARCHAR(255),Thumbnail VARCHAR(50), Description VARCHAR(10000), Published_Date DATETIME,video_duration_seconds INT(100),  Views INT(50), Likes INT(50), Comments INT(50), Favorite_Count INT(50), Definition VARCHAR(50),  Caption_Status VARCHAR(50))''')
cursor.execute('''create table IF NOT EXISTS  comments ( Comment_Id VARCHAR(100), Video_Id VARCHAR(255), Comment_text VARCHAR(10000),  Comment_Author VARCHAR(255), Published_At DATETIME)''')


#####                                               STREAMLIT PART                           #####

# Streamlit App Sidebar Configuration

with st.sidebar:
    st.title(':blue[YouTube Data Harvesting and Warehousing]')
    st.header('Skill Take Away')
    st.caption('Python Scripting')
    st.caption('Data Collection')
    st.caption('API Integration')
    st.caption('Data Management using Pandas and SQL')
    
##  Main Page Title

st.title(':red[YOUTUBE DATA HARVESTING & WAREHOUSING]')


## Data Extraction Header

st.header(':blue[YouTube Data Extraction]')


##  User Input - Channel ID through Streamlit App

channel_id=st.text_input('Enter the Channel ID') 
    

#### converting to DataFrame
try:
    ### channel details into dataframe
    channel_details=get_channel_info(channel_id)
    channel_df=pd.DataFrame(channel_details,index=[0])
    
    # ##  Get list of videos for the channel, extract video details for each video and convert to pandas DF
    video_Ids=get_videos_ids(channel_id)
    video_details=extract_video_info(video_Ids)
    video_df=pd.DataFrame(video_details)
    
    # ##  Get all the comment details for all the videos in the channel and convert to pandas DF
    Comment_Details = get_comment_info(video_Ids)
    comment_df=pd.DataFrame(Comment_Details)
    
    st.success('Channel, Video, Comment Data Successfully Extracted')           ### streamlit message for successfully extracted
    
except KeyError:
    st.warning('Enter a Valid Channel ID')
    
st.header(':blue[YouTube Data Display - After Extraction]')

#####                                                     Display of extracted data in table formate(Data frame)


##  Display the channel data extracted for the input channel ID
if st.button('Channel'):
    st.subheader('Channel Data Table')
    channel_df
    
##  Display the video data extracted for the input channel ID
if st.button('Videos'):
    st.subheader('Video Data Table')
    video_df
    
##  Display the comment data extracted for the input channel ID   
if st.button('Comments'):
    st.subheader('Comment Data Table')
    comment_df
    
    

# ####                                                      LOAD DATA INTO SQL DB

## Data Loading Header
st.header(':blue[YouTube Data Loading]')

## Load the youtube data extracted into SQL data base:
if st.button('Load Data into SQL Database'):
    query = '''select channel_name, Total_Videos from channels
            order by Total_Videos desc'''
    cursor.execute('select channel_id from channels where channel_id=%s', [channel_id])
    connection.commit()
    out = cursor.fetchall()    

    if out:
        st.success('Channel Details of the given channel id already available in Database')
    else:
# Load data into SQL database
        for index, row in channel_df.iterrows(): 
            insert_query='''insert into channels (Channel_Name,
                                        Channel_Id, 
                                        Subscribers, 
                                        Views,
                                        Total_Videos , 
                                        Channel_Description ,
                                        Playlist_Id )
                                        
                                        values(%s,%s,%s, %s,%s,%s,%s)'''
    
            values=(row['Channel_Name'],
            row['Channel_Id'],
            row['Subscribers'],
            row['Views'],
            row['Total_Videos'],
            row['Channel_Description'],
            row['Playlist_Id'])
    
        cursor.execute(insert_query,values)
        connection.commit()
    
        for index, row in video_df.iterrows():
    
            insert_query = '''insert into videos (Channel_Name,
                                        Channel_Id,
                                        Video_Id,
                                        Title,
                                        Thumbnail,
                                        Description,
                                        Published_Date,
                                        video_duration_seconds,
                                        Views,
                                        Likes,
                                        Comments,
                                        Favorite_Count,
                                        Definition,
                                        Caption_Status)
                    values ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
    
            values = (row['Channel_Name'],
                    row['Channel_Id'],
                    row['Video_Id'],
                    row['Title'],
                    row['Thumbnail'],
                    row['Description'],
                    row['formatted_published'],
                    row['video_duration_seconds'],
                    row['Views'],
                    row['Likes'],
                    row['Comments'],
                    row['Favorite_Count'],
                    row['Definition'],
                    row['Caption_Status'])
    
            cursor.execute(insert_query, values)
            connection.commit()
        

    

        for index, row in  comment_df.iterrows():
    
            insert_query = '''insert into comments (Comment_Id,
                                                    Video_Id,
                                                    Comment_Text,
                                                    Comment_Author, 
                                                    Published_At)
                                                    
                                            values ( %s, %s, %s, %s, %s)'''
    
            values = (row['Comment_Id'], 
                        row['Video_Id'] ,
                        row ['Comment_Text'] ,
                        row ['Comment_Author'] , 
                        row ['formatted_published'])
                
            cursor.execute(insert_query, values)
            connection.commit()



    ##### message from streamlit of loaded data to database
    
    st.success('All data extracted for channel, video, comment are loaded into SQL Database Successfully')


####                                                       QUERY DATA FROM DATABASE

## Data Viewing Header
st.header(':blue[YouTube Data from Database - Querying]')

### inserting all Query in streamlit

input_query=st.selectbox('Query DB based on below options', ('1. All Videos and Corresponding Channels',
                                                    '2. Channel with most videos and its number',
                                                    '3. Top 10 most viewed videos and their Channels',
                                                    '4. Comment count of each Video with Channel',
                                                    '5. Videos with highest likes and their channel',
                                                    '6. Total likes for each Video Id and Video name',
                                                    '7. Number of views of each channel with name',
                                                    '8. Names of all channels which published videos in 2022',
                                                    '9. Average Duration of all videos of each channel',
                                                    '10.Videos with highest number of comments with channel'),
                                                    index=None,placeholder='Select your query')

#### usages of IF statements for answering the query

if input_query == '1. All Videos and Corresponding Channels':
    query = '''select channel_Name,Title from videos'''
    cursor.execute(query)
    connection.commit()
    out=cursor.fetchall()
    df_1=pd.DataFrame(out,columns=["Channel_Name","Title"])
    st.write(df_1)
    
    
if input_query == '2. Channel with most videos and its number':
    query = '''select channel_Name, Total_videos from channels
            order by Total_videos desc'''
    cursor.execute(query)
    connection.commit()
    out=cursor.fetchall()
    df_2=pd.DataFrame(out,columns=["Channel Name", "No of Videos"])
    st.write(df_2)   
    
if input_query == '3. Top 10 most viewed videos and their Channels':
    query = '''select channel_Name, Title, Views from videos
            where Views is not null
            order by Views desc limit 100'''
    cursor.execute(query)
    connection.commit()
    out=cursor.fetchall()
    df3=pd.DataFrame(out,columns=["channel_Name","Video Name","Video View Count"])
    st.write(df3)
    
if input_query == '4. Comment count of each Video with Channel':
    query = '''select channel_Name ,Title, Comments from videos
            where Comments is not null'''
    cursor.execute(query)
    connection.commit()
    out=cursor.fetchall()
    df4=pd.DataFrame(out,columns=["channel_Name","Title","Total No of Comments"])
    st.write(df4)   
    
if input_query== '5. Videos with highest likes and their channel':
    query='''select channel_Name,Title,Likes from videos
            where Likes is not null
            order by Likes desc'''
    cursor.execute(query)
    connection.commit()
    out=cursor.fetchall()
    df5=pd.DataFrame(out,columns=["channel_Name","Title","Likes"])
    st.write(df5)
    
if input_query== '6. Total likes for each Video Id and Video name':
    query='''select channel_Name,Title, Likes from videos
            where Likes is not null
            order by Likes desc'''
    cursor.execute(query)
    connection.commit()
    out=cursor.fetchall()
    df6=pd.DataFrame(out,columns=["channel_Name","Title","Likes"])
    st.write(df6)
    
    
if input_query == '7. Number of views of each channel with name':
    query = '''select channel_Name, Channel_ID, Views from channels
            where Views is not null
            order by Views desc'''
    cursor.execute(query)
    connection.commit()
    out=cursor.fetchall()
    df7=pd.DataFrame(out,columns=["channel_Name","Channel_ID ","Video View Count"])
    st.write(df7)
    
if input_query == '8. Names of all channels which published videos in 2022':
    query = '''Select channel_Name, Title, Published_Date from videos
                where extract(YEAR from Published_Date) = 2022'''
    cursor.execute(query)
    connection.commit()
    out=cursor.fetchall()
    df8=pd.DataFrame(out,columns=["Channel Name","Title","Published_Date"])
    st.write(df8)
    
if input_query == '9. Average Duration of all videos of each channel':
    query = '''select channel_Name, avg(video_duration_seconds) as AverageDuration from videos
            group by channel_Name'''
    cursor.execute(query)
    connection.commit()
    out=cursor.fetchall()
    df9=pd.DataFrame(out,columns=["Channel Name","Average Video Duration in seconds"])
    st.write(df9)
    
if input_query == '10.Videos with highest number of comments with channel':
    query = '''select channel_name ,Title, Comments from videos
            where Comments is not null
            order by Comments desc'''
    cursor.execute(query)
    connection.commit()
    out=cursor.fetchall()
    df10=pd.DataFrame(out,columns=["Channel Name","Video Name","Total No of Comments"])
    st.write(df10)
    
    
    
    ######                                       END OF THE coding                        #########
