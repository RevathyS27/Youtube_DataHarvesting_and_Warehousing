import googleapiclient.discovery
import pymongo
import psycopg2
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu


api_service_name = "youtube"
api_version = "v3"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey="AIzaSyAVQe1FRchb2efA0N7H_zbN6kqLTQVxNEg")

#Get Channel Data
def get_channel_data(ch_id):
  c=[]
  channel_request = youtube.channels().list(
          part="snippet,contentDetails,statistics",
          id=ch_id
      )
  channel_response = channel_request.execute()
  for i in channel_response['items']:
    channel_data={
        "Channel_name":i['snippet']['title'],
        "Channel_ID":i['id'],
        "Channel_Subscriber":i['statistics']['subscriberCount'],
        "Total_view":i['statistics']['viewCount'],
        "Total_videos":i['statistics']['videoCount'],
        "Channel_description":i['snippet']['description'],
        "Playlist_Id":i['contentDetails']['relatedPlaylists']['uploads']
    }
    c.append(channel_data)
    return c
  
#Get video_Id
def get_video_id(ch_id):
  v=[]
  playlistID_request = youtube.channels().list(
          part='contentDetails',
          id=ch_id
      )
  playlistID_response=playlistID_request.execute()
  playlist_id = playlistID_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
  #print(playlist_id)
  next_page_Token=None
  while True:
    videoID_request = youtube.playlistItems().list(
         part='snippet',
         playlistId=playlist_id,
         maxResults=50,
         pageToken=next_page_Token
        )
    videoID_response=videoID_request.execute()   
    for j in range(len(videoID_response['items'])):
      v_id=videoID_response['items'][j]['snippet']['resourceId']['videoId']
      v.append(v_id)
    next_page_Token=videoID_response.get('nextPageToken')
    if next_page_Token is None:
      break
  return v

#Video_Details:
def get_video_data(vid):
  vd=[]
  for i in vid:
    VideoData_request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=i
        )
    VideoData_response = VideoData_request.execute()
    for l in VideoData_response['items']:
      video_data={
          "Channel_name":l['snippet']['channelTitle'],
          "Channel_ID":l['snippet']['channelId'],
          "Video_ID":l['id'],
          "Title":l['snippet']['title'],
          "Tags":l['snippet'].get('tags'),
          "Thumbnails":l['snippet']['thumbnails']['default']['url'],
          "Description":l['snippet'].get('description'),
          "Published_Date":l['snippet']['publishedAt'],
          "Duration":l['contentDetails']['duration'],
          "Video_Views":l['statistics'].get('viewCount'),
          "video_likes":l['statistics'].get('likeCount'),
          "video_dislikes":l['statistics'].get('dislikeCount'),
          "Video_Comments":l['statistics'].get('commentCount'),
          "Favourite_count":l['statistics']['favoriteCount'],
          "Definition":l['contentDetails']['definition'],
          "Caption_Status":l['contentDetails']['caption']
      }
      vd.append(video_data)
  return vd

#Get Comment_Info
def get_comment_data(vid_id):
  comment=[]
  try:
    for n in vid_id:
      comment_request = youtube.commentThreads().list(
                part="snippet,replies",
                videoId=n,
                maxResults=50
            )
      comment_response = comment_request.execute()
      for m in comment_response['items']:
        comment_data={
            "comment_id":m['id'],
            "video_id":m['snippet']['videoId'],
            "comment_text":m['snippet']['topLevelComment']['snippet']['textDisplay'],
            "comment_author":m['snippet']['topLevelComment']['snippet']['authorDisplayName'],
            "comment_published_date":m['snippet']['topLevelComment']['snippet']['publishedAt'] }
        comment.append(comment_data)
  except:
    pass
  return comment


#Inserting data into MongoDB
#Create Connection:
conn=pymongo.MongoClient("mongodb://Revathy:guvi2024@ac-5n8fsfk-shard-00-00.berjwr1.mongodb.net:27017,ac-5n8fsfk-shard-00-01.berjwr1.mongodb.net:27017,ac-5n8fsfk-shard-00-02.berjwr1.mongodb.net:27017/?ssl=true&replicaSet=atlas-11ihco-shard-0&authSource=admin&retryWrites=true&w=majority&appName=Cluster0")

#Create Database
Youtube_DB=conn['Youtube_Database']

#Creating Collection and Document
def channel_detail(channel_id):
    ch_data=get_channel_data(channel_id)
    video_id=get_video_id(channel_id)
    video_info=get_video_data(video_id)
    c_data=get_comment_data(video_id)
    col1=Youtube_DB["Channel_Details"] 
    col1.insert_one({'Channel_Information':ch_data,
                      'Video_Information':video_info,'Comment_Information':c_data})  
    return "Data has been Succesfully Uploaded"

#Channel_Table in SQL
def channel_table():
    #importing
    import psycopg2

    # Establish a connection to your MySQL database
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="Revathy@27",
        database="you"
    )

    # Create a cursor object
    mycursor = mydb.cursor()
    drop_query='''drop table if exists channels'''
    mycursor.execute(drop_query)
    mydb.commit()
    try:
        # Define the CREATE TABLE query
        create_query = '''
            CREATE TABLE IF NOT EXISTS channels (
                channel_name VARCHAR(100),
                channel_ID VARCHAR(80) PRIMARY KEY,
                total_view BIGINT,
                total_subscribers BIGINT,
                total_videos INT,
                channel_description TEXT,
                playlist_id VARCHAR(80)
            )
        '''

        # Execute the query
        mycursor.execute(create_query)

        # Commit the changes
        mydb.commit()
    except:
        print("Table already exist")
    

   #Extracting data from MongoDB
    ch_list=[]
    Youtube_DB=conn["Youtube_Database"]
    col1=Youtube_DB["Channel_Details"] 
    for ch_d in col1.find({},{"_id":0,"Channel_Information":1}):
        for i in  range(len(ch_d['Channel_Information'])):
            ch_list.append(ch_d['Channel_Information'][i])
    #Making the extracted data as dataframe
    df=pd.DataFrame(ch_list)
    #Mapping with postgeral
    for i,row in df.iterrows():
        #print(i,row)
        insert_query='''insert into channels( channel_name,
                                            channel_ID,
                                            total_view,
                                            total_subscribers,
                                            total_videos,
                                            channel_description,
                                            playlist_id)
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_name'],
                row['Channel_ID'],
                row['Channel_Subscriber'],
                row['Total_view'],
                row['Total_videos'],
                row['Channel_description'],
                row['Playlist_Id'])
    # Connect to your database and execute the query
        try:
            mycursor.execute(insert_query,values)
            mydb.commit()
            print("Record inserted successfully!")
        except:
            print("Record already inserted")


#Video Table
def video_table():
    # Establish a connection to your MySQL database
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="Revathy@27",
        database="you"
    )

    # Create a cursor object
    mycursor = mydb.cursor()
    drop_query='''drop table if exists videos'''
    mycursor.execute(drop_query)
    mydb.commit()
    try:
        # Define the CREATE TABLE query
        create_query = '''
            CREATE TABLE IF NOT EXISTS videos (Channel_name varchar(100),
                                                Channel_ID varchar(100),
                                                Video_ID varchar(100) primary key,
                                                Title varchar(150),
                                                Tags text,
                                                Thumbnails varchar(200),
                                                Description text,
                                                Published_Date timestamp,
                                                Duration interval,
                                                Video_Views bigint,
                                                video_likes bigint,
                                                video_dislikes bigint,
                                                Video_Comments int,
                                                Favourite_count int,
                                                Definition varchar(100),
                                                Caption_Status varchar(100))
        '''

        # Execute the query
        mycursor.execute(create_query)

        # Commit the changes
        mydb.commit()
    except:
        print("Table already exist")

        
    #Extracting data from MongoDB
    vi_list=[]
    Youtube_DB=conn["Youtube_Database"]
    col1=Youtube_DB["Channel_Details"] 
    for vi_d in col1.find({},{"_id":0,"Video_Information":1}):
        for i in range(len(vi_d['Video_Information'])):
            vi_list.append(vi_d['Video_Information'][i])
    #Making the extracted data as dataframe
    df2=pd.DataFrame(vi_list)


    #Mapping with postgeral
    for i,row in df2.iterrows():
        #print(i,row)
        insert_query='''insert into videos (Channel_name,
                                            Channel_ID,
                                            Video_ID,
                                            Title,
                                            Tags,
                                            Thumbnails,
                                            Description,
                                            Published_Date,
                                            Duration,
                                            Video_Views,
                                            video_likes ,
                                            video_dislikes,
                                            Video_Comments,
                                            Favourite_count,
                                            Definition,
                                            Caption_Status   
                                            )
                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
    
        values=(row['Channel_name'],
                row['Channel_ID'],
                row['Video_ID'],
                row['Title'],
                row['Tags'],
                row['Thumbnails'],
                row['Description'],
                row['Published_Date'],
                row['Duration'],
                row['Video_Views'],
                row['video_likes'],
                row['video_dislikes'],
                row['Video_Comments'],
                row['Favourite_count'],
                row['Definition'],
                row['Caption_Status']
                )
        try:
            #Connect to your database and execute the query
            mycursor.execute(insert_query,values)
            mydb.commit()
            print("Video_Record inserted successfully!")
        except:
            print("Video_record already inserted")
        
def comment_table():
    #Comment Table
    # Establish a connection to your MySQL database
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="Revathy@27",
        database="you"
    )

    # Create a cursor object
    mycursor = mydb.cursor()
    drop_query='''drop table if exists comments'''
    mycursor.execute(drop_query)
    mydb.commit()
    try:
        # Define the CREATE TABLE query
        create_query = '''
            CREATE TABLE IF NOT EXISTS comments(comment_id varchar(100) primary key,
                                                video_id varchar(100),
                                                comment_text text,
                                                comment_author varchar(100) ,
                                                comment_published_date timestamp
                                                    
            )
        '''

        # Execute the query
        mycursor.execute(create_query)

        # Commit the changes
        mydb.commit()
    except:
        print("Table already exist")


    #Extracting data from MongoDB
    com_list=[]
    Youtube_DB=conn["Youtube_Database"]
    col1=Youtube_DB["Channel_Details"] 
    for com_d in col1.find({},{"_id":0,"Comment_Information":1}):
        for i in range(len(com_d['Comment_Information'])):
            com_list.append(com_d['Comment_Information'][i])
    #Making the extracted data as dataframe
    df3=pd.DataFrame(com_list)

    #Mapping with postgeral
    for i,row in df3.iterrows():
        #print(i,row)
        insert_query='''insert into comments(comment_id,
                                            video_id,
                                            comment_text,
                                            comment_author,
                                            comment_published_date
                                            )
                                            values(%s,%s,%s,%s,%s)'''
        values=(row['comment_id'],
                row['video_id'],
                row['comment_text'],
                row['comment_author'],
                row['comment_published_date'])
        
        try:
            mycursor.execute(insert_query,values)
            mydb.commit()
            print("Comment_Record inserted successfully!")
        except:
            print("Comment_Record already inserted")

#Defining function for all tables
def tables():
    channel_table()
    video_table()
    comment_table()
    return 'Table created and Migrated to SQL Database'



#Streamlit Part
st.title(":purple[Youtube Data Harvesting and warehousing]")
selected = option_menu(
    menu_title="",  # Leave it empty since we've already displayed the title above
    options=[
        "Home", 
        "Extract and Upload", 
        "Query_Analysis"
    ],
    orientation="horizontal"
)
if selected =="Home":
    st.markdown("The goal of the YouTube Data Harvesting and Warehousing project is to give consumers access to and analysis of data from several YouTube channels. The project uses Streamlit, SQL, and MongoDB to create an intuitive application that lets users to retrieve, save, and query YouTube channel and video data.")
if selected =="Extract and Upload":
    #Get_User_input
    tab1,tab2=st.tabs(["Collect and store in Mongodb","Migrate to SQL"])
    with tab1:
        channel_ID=st.text_input("Enter the Channel_ID:")


        #Store to mongoDB database
        store_Data=st.button("Extract and Upload")
        if store_Data:
            ch_ids=[]
            Youtube_DB=conn["Youtube_Database"]
            col1=Youtube_DB["Channel_Details"]
            for ch_data in col1.find({},{"_id":0,"Channel_Information":1}):
                for i in  range(len(ch_data['Channel_Information'])):
                    ch_ids.append(ch_data['Channel_Information'][i]['Channel_ID'])
            if channel_ID in ch_ids:
                st.error("The given channel detail already exist in Database")
            else:
                inserted=channel_detail(channel_ID)
                st.success(inserted)
    with tab2:
        #Migrate to SQL
        SQL=st.button("Migrate to SQL")
        if SQL:
            Tables=tables()
            st.success(Tables)

if selected =="Query_Analysis":
    #SQL_Query
    Questions=st.selectbox("Select your Question",(
                                                "1.Name of all the videos and their corresponding channels",
                                                "2.Channel that have most number of videos and the count of the videos",
                                                "3.Top 10 viewed videos and their corresponding channels",
                                                "4.Total number of comment on each video and their respective video name",
                                                "5.Videos that have the highest number of likes and their corresponding channel name",
                                                "6.Total number of likes and dislikes on each video and their corresponding video name",
                                                "7.Total number of views for each channel and their respective channel name ",
                                                "8.Names of all the channel that have published video in the year 2022",
                                                "9.Average duration of all videos in each channel and their corresponding channel name",
                                                "10.Videos having highest number of comments and their corresponding channel name"

    ))
    mydb = psycopg2.connect(
            host="localhost",
            user="postgres",
            password="Revathy@27",
            database="you"
        )
    mycursor = mydb.cursor()
    if Questions=="1.Name of all the videos and their corresponding channels":
        query1=''' select Title as Video_name,Channel_name from videos'''
        mycursor.execute(query1)
        mydb.commit()
        t1=mycursor.fetchall()
        df1=pd.DataFrame(t1,columns=["Video_name","Channel_name"])
        st.write(df1)
    elif Questions=="2.Channel that have most number of videos and the count of the videos":
        query2=''' select channel_name, total_videos from channels order by total_videos desc'''
        mycursor.execute(query2)
        mydb.commit()
        t1=mycursor.fetchall()
        df2=pd.DataFrame(t1,columns=["channel_name","total_videos"])
        st.write(df2)
    elif Questions=="3.Top 10 viewed videos and their corresponding channels":
        query3=''' select title,channel_name,video_views from videos order by video_views desc limit 10'''
        mycursor.execute(query3)
        mydb.commit()
        t1=mycursor.fetchall()
        df3=pd.DataFrame(t1,columns=["Video_title","Channel Name","Video_views"])
        st.write(df3)
    elif Questions=="4.Total number of comment on each video and their respective video name":
        query4=''' select title,video_comments from videos'''
        mycursor.execute(query4)
        mydb.commit()
        t1=mycursor.fetchall()
        df4=pd.DataFrame(t1,columns=["Video_title","Video_comments"])
        st.write(df4)
    elif  Questions=="5.Videos that have the highest number of likes and their corresponding channel name":
        query5=''' select title,channel_name,video_likes from videos where video_likes is not null order by video_likes desc'''
        mycursor.execute(query5)
        mydb.commit()
        t1=mycursor.fetchall()
        df5=pd.DataFrame(t1,columns=["Video_title","Channel Name","Video_likes"])
        st.write(df5) 
    elif  Questions=="6.Total number of likes and dislikes on each video and their corresponding video name":
        query6=''' select title,video_likes from videos '''
        mycursor.execute(query6)
        mydb.commit()
        t1=mycursor.fetchall()
        df6=pd.DataFrame(t1,columns=["Video_title","Video_likes"])
        st.write(df6) 
    elif Questions=="7.Total number of views for each channel and their respective channel name ":
        query7=''' select channel_name,total_view from channels '''
        mycursor.execute(query7)
        mydb.commit()
        t1=mycursor.fetchall()
        df7=pd.DataFrame(t1,columns=["Channel_name","Total_view"])
        st.write(df7)  
    elif Questions=="8.Names of all the channel that have published video in the year 2022":
        query8='''select title,channel_name,published_date from videos where extract(year from published_date)=2022'''
        mycursor.execute(query8)
        mydb.commit()
        t1=mycursor.fetchall()
        df8=pd.DataFrame(t1,columns=["Channel_name","Year_Published","Title"])
        st.write(df8)  
    elif Questions=="9.Average duration of all videos in each channel and their corresponding channel name":
        query9='''select channel_name,AVG(duration) as avg_duration from videos group by channel_name'''
        mycursor.execute(query9)
        mydb.commit()
        t1=mycursor.fetchall()
        df9=pd.DataFrame(t1,columns=["Channel_name","Avg_Duration"])
        t9=[]
        for ind,row in df9.iterrows():
            channel_title=row["Channel_name"]
            channel_avg_duration=row["Avg_Duration"]
            channel_avg_duration_str=str(channel_avg_duration)
            t9.append({"Channel_name":channel_title,"Channel_Avg_Duration":channel_avg_duration_str})
        df=pd.DataFrame(t9)
        st.write(df)  
    elif  Questions=="10.Videos having highest number of comments and their corresponding channel name":
        query10='''select title,channel_name,video_comments from videos where video_comments is not null order by video_comments  desc'''
        mycursor.execute(query10)
        mydb.commit()
        t10=mycursor.fetchall()
        df10=pd.DataFrame(t10,columns=["Title","Channel_name","video_comments"])
        st.write(df10)  
        

        


        


    
