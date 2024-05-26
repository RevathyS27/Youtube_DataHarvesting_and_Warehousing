from googleapiclient.discovery import build
import pymongo
import pandas as pd
import psycopg2
import streamlit as st


api_id="AIzaSyAVQe1FRchb2efA0N7H_zbN6kqLTQVxNEg"
api_service_name="youtube"
api_version="v3"
youtube=build(api_service_name,api_version,developerKey=api_id)


#to get channel details
def channel_data(ch_id):
  ch=[]
  request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=ch_id
    )
  response = request.execute()
  for i in response['items']:
    ch_data = {
        'Channel_name':i['snippet']['title'],
        'Channel_ID':i['id'],
        'Subscription_Count': i['statistics']['subscriberCount'],
        'Channel_Views':i['statistics']['viewCount'],
        'Total_videos':i['statistics']['videoCount'],
        'Channel_Description':i['snippet']['description'],
        'Playlist_ID':i['contentDetails']['relatedPlaylists']['uploads']
    }
    ch.append(ch_data)
  return ch

  
#to get the video id's using the playlist id
def video_id(channel_id):
  video_ids=[]
  next_page_token=None
  #to get playlist id
  request=youtube.channels().list(
    part="contentDetails",
    id=channel_id
)
  response=request.execute()
  playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
  playlist_id
  while (True):
    request1=youtube.playlistItems().list(
        part="snippet",
        playlistId=playlist_id,
        maxResults=50,
        pageToken=next_page_token
    )
    response1=request1.execute()

    for i in range(len(response1['items'])):
      v_id=response1['items'][i]['snippet']['resourceId']['videoId']
      video_ids.append(v_id)
    next_page_token=response1.get('nextPageToken')

    if next_page_token is None:
      break

  return video_ids

  #to get video details
def video_details(video_ids):
  V=[]
  for video in video_ids:
    request = youtube.videos().list(
        part='snippet,contentDetails,statistics',
        id=video
      )
    response = request.execute()
    for i in response['items']:
      video_data={
            "Channel_name":i['snippet']['channelTitle'],
            "Channel_ID":i['snippet']['channelId'],
            "Video_Id":i['id'] ,
            "Title":i['snippet']['title'],
            "Video_Description":i['snippet'].get('description') ,
            "Tags":i['snippet'].get('tags'),
            "PublishedAt":i['snippet']['publishedAt'] ,
            "View_Count":i['statistics']['viewCount'] ,
            "like_count" : i['statistics'].get('likeCount'),
            "Favorite_Count":i['statistics'].get('favoriteCount'),
            "Comment_Count":i['statistics'].get('commentCount') ,
            "Duration":i['contentDetails']['duration'] ,
            "Thumbnail":i['snippet']['thumbnails']['default'].get('url'),
            "Definition":i['contentDetails']['definition'],
            "Caption_Status":i['contentDetails']['caption']
          }
      V.append(video_data)
  return V


#to get comment details
def comment_details(v):
  c=[]
  try:
    for i in v:
      request = youtube.commentThreads().list(
          part='snippet,replies',
          videoId=i,
          maxResults=50
      )
      response = request.execute()
      for j in response['items']:
        comment_data={
          "Comment_Id":j['id'],
          "Video_Id":j['snippet']['videoId'],
          "Comment_Text":j['snippet']['topLevelComment']['snippet']['textDisplay'] ,
          "Comment_Author":j['snippet']['topLevelComment']['snippet']['authorDisplayName'] ,
          "Comment_PublishedAt":j['snippet']['topLevelComment']['snippet']['publishedAt']

        }
        c.append(comment_data)
  except:
    pass
  return c

conn=pymongo.MongoClient("mongodb://Revathy:guvi2024@ac-5n8fsfk-shard-00-00.berjwr1.mongodb.net:27017,ac-5n8fsfk-shard-00-01.berjwr1.mongodb.net:27017,ac-5n8fsfk-shard-00-02.berjwr1.mongodb.net:27017/?ssl=true&replicaSet=atlas-11ihco-shard-0&authSource=admin&retryWrites=true&w=majority&appName=Cluster0")

db=conn["youtube"]

def channel_details(channel_id):
  ch_info=channel_data(channel_id)
  v_id=video_id(channel_id)
  v_details=video_details(v_id)
  c_details=comment_details(v_id)
  coll=db["youtube_data"]
  coll.insert_one({"Channel_info":ch_info,"Video IDs":v_id,"Video Details":v_details,"Comment Info":c_details})

  return "Data uploaded successfully"

#SQL
mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="Revathy@27",
        database="you"
    )

# Create a cursor object
mycursor = mydb.cursor()

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
    print("Iam going to drop the table")
    drop_query='''drop table if exists channels'''
    mycursor.execute(drop_query)
    mydb.commit()
    print("Table dropped success")
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
    Youtube_DB=conn["youtube"]
    col1=Youtube_DB["youtube_data"] 
    for ch_d in col1.find({},{"_id":0,"Channel_info":1}):
        for i in  range(len(ch_d['Channel_info'])):
            ch_list.append(ch_d['Channel_info'][i])
    #Making the extracted data as dataframe
    df=pd.DataFrame(ch_list)
    #Mapping with postgeral
    for i,row in df.iterrows():
        #print(i,row)
        insert_query='''insert into channels(channel_name,
                                            channel_ID,
                                            total_view,
                                            total_subscribers,
                                            total_videos,
                                            channel_description,
                                            playlist_id)
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_name'],
                row['Channel_ID'],
                row['Channel_Views'],
                row['Subscription_Count'],
                row['Total_videos'],
                row['Channel_Description'],
                row['Playlist_ID'])
    # Connect to your database and execute the query
        try:
            mycursor.execute(insert_query,values)
            mydb.commit()
            print("Record inserted successfully!")
        except:
            print("Record already inserted")

    
#Video Table
def video_table():
    # Establish a connection to your PostgreSQL database
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="Revathy@27",
        database="you"
    )

    # Create a cursor object
    mycursor = mydb.cursor()
    try:
    # Drop the table if it already exists
        mycursor.execute('''DROP TABLE IF EXISTS videos''')
        mydb.commit()
        print("Table dropped successfully")
    except Exception as drop_err:
        print("Error dropping table:", drop_err)

    try:
        # Define the CREATE TABLE query
        create_query = '''
            CREATE TABLE IF NOT EXISTS videos (
                Channel_name VARCHAR(100),
                Channel_ID VARCHAR(100),
                Video_ID VARCHAR(100) PRIMARY KEY,
                Title VARCHAR(150),
                Tags TEXT,
                Thumbnails VARCHAR(200),
                Description TEXT,
                Published_Date TIMESTAMP,
                Duration INTERVAL,
                Video_Views BIGINT,
                Video_Likes BIGINT,
                Video_Dislikes BIGINT,
                Video_Comments INT,
                Favourite_Count INT,
                Definition VARCHAR(100),
                Caption_Status VARCHAR(100)
            )
        '''

        # Execute the query
        mycursor.execute(create_query)
        mydb.commit()
    except Exception as e:
        print("Error creating table:", e)

    # Extracting data from MongoDB
    vi_list = []
    Youtube_DB = conn["youtube"]
    col1 = Youtube_DB["youtube_data"]
    for vi_d in col1.find({}, {"_id": 0, "Video Details": 1}):
        for i in range(len(vi_d['Video Details'])):
            vi_list.append(vi_d['Video Details'][i])

    # Mapping with PostgreSQL
    for row in vi_list:
        insert_query = '''
            INSERT INTO videos (
                Channel_name, Channel_ID, Video_ID, Title, Tags, Thumbnails, Description,
                Published_Date, Duration, Video_Views, Video_Likes, Video_Comments,
                Favourite_Count, Definition, Caption_Status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (Video_ID) DO NOTHING
        '''

        values = (
            row['Channel_name'],
            row['Channel_ID'],
            row['Video_Id'],
            row['Title'],
            row['Tags'],
            row['Thumbnail'],
            row['Video_Description'],
            row['PublishedAt'],
            row['Duration'],
            row['View_Count'],
            row['like_count'],
            row['Comment_Count'],
            row['Favorite_Count'],
            row['Definition'],
            row['Caption_Status']
        )

        try:
            # Connect to your database and execute the query
            mycursor.execute(insert_query, values)
            mydb.commit()
            print("Video_Record inserted successfully!")
        except Exception as e:
            print("Error inserting video record:", e)

        

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
    print("Iam going to drop the table")
    drop_query='''drop table if exists comments'''
    mycursor.execute(drop_query)
    mydb.commit()
    print("Table dropped success")

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
    db=conn["youtube"]
    coll=db["youtube_data"]
    for com_d in coll.find({},{"_id":0,'Comment Info':1}):
        for i in range(len(com_d['Comment Info'])):
            com_list.append(com_d['Comment Info'][i])
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
        values=(row['Comment_Id'],
        row['Video_Id'],
        row['Comment_Text'],
        row['Comment_Author'],
        row['Comment_PublishedAt'])

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
    #return 'Table created and Migrated to SQL Database'


    
#Streamlit Part
st.title("Youtube Data Harvesting and warehousing")


tab1, tab2 = st.tabs(["EXTRACT_DATA","ANALYSIS"])
with tab1:
    #Get_User_input
    channel_ID=st.text_input("Enter the Channel_ID:")

    #Store to mongoDB database
    store_Data=st.button("Collect and store Data")
    if store_Data:
        ch_ids=[]
        db=conn['youtube']
        coll=db["youtube_data"]
        for ch_d in coll.find({},{"_id":0,"Channel_info":1}):
            for i in  range(len(ch_d['Channel_info'])):
                ch_ids.append(ch_d['Channel_info'][i]['Channel_ID'])
        if channel_ID in ch_ids:
            st.error("The given channel detail already exist in Database")
        else:
            inserted=channel_details(channel_ID)
            st.success("Succesfully Loaded to MongoDB")
    
    #Migrate to SQL
    SQL=st.button("Migrate to SQL")
    if SQL:
        tables()
        st.success("Videos migrated to SQL successfully.")
    

with tab2:
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

  
    mycursor = mydb.cursor()
    if Questions=="1.Name of all the videos and their corresponding channels":
        query1 = '''SELECT Title AS Video_name, Channel_name FROM videos'''
        mycursor.execute(query1)
        t1 = mycursor.fetchall()
        df = pd.DataFrame(t1, columns=["Video_name", "Channel Name"])
        st.write(df)
    elif Questions=="2.Channel that have most number of videos and the count of the videos":
        query2=''' select channel_name, total_videos from channels order by total_videos desc'''
        mycursor.execute(query2)
        t1 = mycursor.fetchall()
        df2=pd.DataFrame(t1,columns=["Channel Name","total_videos"])
        st.write(df2)
    elif Questions=="3.Top 10 viewed videos and their corresponding channels":
        query3=''' select Channel_name,Title,Video_Views from videos order by Video_Views desc limit 10'''
        mycursor.execute(query3)
        t1 = mycursor.fetchall()
        df3=pd.DataFrame(t1,columns=["Channel Name","Video_title","View_Count"])
        st.write(df3)
    elif Questions=="4.Total number of comment on each video and their respective video name":
        query4=''' select Title,Video_Comments from videos'''
        mycursor.execute(query4)
        t1 = mycursor.fetchall()
        df4=pd.DataFrame(t1,columns=["Video_title","Comment_Count"])
        st.write(df4)
    elif  Questions=="5.Videos that have the highest number of likes and their corresponding channel name":
        query5=''' select Channel_name,Title,video_likes from videos where video_likes is not null order by video_likes desc'''
        mycursor.execute(query5)
        t1 = mycursor.fetchall()
        df5=pd.DataFrame(t1,columns=["Channel Name","Video Title","Like_Count"])
        st.write(df5) 
    elif  Questions=="6.Total number of likes and dislikes on each video and their corresponding video name":
        query6=''' select Title,video_likes from videos '''
        mycursor.execute(query6)
        t1 = mycursor.fetchall()
        df6=pd.DataFrame(t1,columns=["Video_title","Like_Count"])
        st.write(df6) 
    elif Questions=="7.Total number of views for each channel and their respective channel name ":
        query7='''select  channel_name,total_view from channels'''
        mycursor.execute(query7)
        t1 = mycursor.fetchall()
        df7=pd.DataFrame(t1,columns=[" Channel_name","Channel_Views"])
        st.write(df7)  
    elif Questions=="8.Names of all the channel that have published video in the year 2022":
        query8='''select Title,Channel_name,Published_Date from videos where extract(year from Published_Date)=2022'''
        mycursor.execute(query8)
        t1 = mycursor.fetchall()
        df8=pd.DataFrame(t1,columns=["Title","Channel_name","Year_Published"])
        st.write(df8)  
    elif Questions=="9.Average duration of all videos in each channel and their corresponding channel name":
        query9='''select Channel_name,AVG(Duration) as avg_duration from videos group by Channel_name'''
        mycursor.execute(query9)
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
        query10='''select Title,Channel_name,Video_Comments from videos where Video_Comments is not null order by Video_Comments  desc'''
        mycursor.execute(query10)
        t10=mycursor.fetchall()
        df10=pd.DataFrame(t10,columns=["Title","Channel_name","video_comments"])
        st.write(df10)  
        

        
#UC7FBe_HWJDeDw4jsC_8Rg9g
#UChNLWg-q2SkYDw4bR8JK4Xg
#UCiuLs5TAg1pY3Iylgcx916g