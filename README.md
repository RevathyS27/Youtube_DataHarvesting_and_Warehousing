# Youtube_DataHarvesting_and_Warehousing

Introduction:
          My first project is a Streamlit programme that allows users to evaluate data from multiple YouTube channels. It is called Youtube data harvesting and warehousing utilising Sqland Streamlit. To access information such as channel details, video details, and user engagement, users must provide their YouTube channel ID. The software need to make it easier for users to save data in a MongoDB database and enable them to gather information from up to ten distinct sources. To facilitate additional analysis, it should also have the ability to move specific channel data from the data lake to a SQL database. The application ought to facilitate data retrieval and searching from the SQL database, including sophisticated features such as table joining to obtain extensive channel details.
          
Tools and Libraries Used:
          1.Google Client Library
          2.Python
          3.MongoDB
          4.MySQL
          5.streamlit

Process:
     1.Data Retrieval: Use the Google API to retrieve relevant information by entering the ID of a YouTube channel.
     2.Data Storage: As a data lake, store the gathered data in a MongoDB database.
     3.Data Gathering: Gather information from ten distinct YouTube channels and store it in the data lake.
     4.Data Migration: Transfer data in a table-organized SQL database from the data lake.
     5.Data Search and Retrieval: Use a variety of search parameters, such as connecting tables to obtain complete channel 
      details, to search and retrieve data from the SQL database. Application Enter the ID of a YouTube channel to get 
      pertinent information from the Google API.To store the data in the MongoDB data lake, click the "Collect Data" button.
      To transfer data from the data lake to the SQL database, choose a channel name.
Investigate the many options and quickly analyse YouTube channel statistics.
      
