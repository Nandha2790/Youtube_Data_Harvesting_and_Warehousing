# Youtube Data Harvesting and Warehousing
## Overview
This project aims to harvest data from YouTube channels using the YouTube API and store it in MongoDB. The harvested data includes channel information, playlist data, video data, and comment data. After cleaning the data, it is structured and stored in MongoDB. Additionally, the project provides functionality to export the data to MySQL for further analysis.

## Features
Import YouTube channel data to MongoDB using the Google API.
Clean and structure the imported data.
Export the cleaned data to MySQL for further analysis.
View and query the data in the Streamlit app.
## Build with
- Python
- Streamlit
- Google API Client
- MongoDB
- MySQL
- pandas
- sqlalchemy
- isodate
- pymongo
- pymysql
## Settingup Instructions
- Clone the repository to your local machine using `git clone https://github.com/Nandha2790/Youtube_Data_Harvesting_and_Warehousing` .
- Install Python (if not already installed).
- Install the required libraries.
- Obtain a Google API key for the YouTube API from the Google Developers Console.
- Ensure MongoDB is installed and running on your machine.
- Ensure MySQL is installed and running on your machine.

## Running the Application
- Open a terminal and navigate to the project directory.
- Run the application using the command `streamlit run main.py`.
- Access the Streamlit app in your web browser by opening the link displayed in the terminal (usually http://localhost:8501).

## Importing Data to MongoDB
- Enter the YouTube channel ID and API key in the Streamlit sidebar.
- Click the "Import Data to MongoDB" button to import the data from the specified YouTube channel to MongoDB.
- The imported data includes channel information, playlist data, video data, and comment data.

## Exporting Data to MySQL
- Use the "Export Data to MySQL" tab in the Streamlit app to export the cleaned data from MongoDB to MySQL.
- Specify the MySQL database connection details (host, port, database name, username, password).
- Click the "Export Data" button to transfer the data to MySQL.

## Viewing and Querying Data
- The "Channel Data" tab displays information about the imported YouTube channel.
- The "Export to SQL" tab displays the imported youtube channel list where user can select a channel and saved its data to MySQL server.
- Use the "10 SQL Queries" tab to view the selected queries.

# Script Concept

## API Integration:
- The script integrates with the YouTube API using the google-api-python-client library to fetch data from a specified YouTube channel.
- The API key is used to authenticate requests to the YouTube API.

## MongoDB Storage:
- The script uses the pymongo library to connect to a MongoDB database and store the processed data.
- Data is stored in MongoDB collections corresponding to the different types of data (e.g., channels, playlists, videos, comments).
    ## Channel Data Extraction

    This script uses the YouTube API to extract data about a specific YouTube channel. Here's a step-by-step explanation of the process:

    1. **YouTube API Request**: The script sends a request to the YouTube API to retrieve information about a specific YouTube channel. The request includes the channel ID and specifies the types of data to be retrieved: 'snippet', 'contentDetails', 'statistics', and 'status'.

    2. **API Response**: The script executes the API request and retrieves the response, which is a dictionary containing information about the requested YouTube channel.

    3. **Channel Name**: The script extracts the name of the YouTube channel from the API response.

    4. **Channel Table**: The script creates a dictionary to store the extracted channel data. The dictionary includes the following key-value pairs:
    - `"channel_id"`: The ID of the YouTube channel.
    - `"channel_name"`: The name of the YouTube channel.
    - `"channel_description"`: The description of the YouTube channel.
    - `"channel_type"`: The type of the YouTube channel. If the word "vlogs" appears in the channel's description, the type is set to "Vlogs and Travel". Otherwise, the type is set to `None`.
    - `"channel_views"`: The total number of views of the YouTube channel.
    - `"channel_status"`: The privacy status of the YouTube channel.

    ## Playlist Data Extraction

    This script uses the YouTube API to extract data about the playlists of a specific YouTube channel. Here's a step-by-step explanation of the process: 
            `"def pl_table(channel_id): # function to get playlist table`  

    1. **YouTube API Request**: The script sends a request to the YouTube API to retrieve information about the playlists of a specific YouTube channel. The request includes the channel ID and specifies the types of data to be retrieved: 'snippet' and 'contentDetails'.

    2. **API Response**: The script executes the API request and retrieves the response, which is a dictionary containing information about the playlists of the requested YouTube channel.

    3. **Extract Playlist Data**: The script iterates over the items in the API response, and for each item, it extracts the channel ID, playlist ID, and playlist name, and stores these in a dictionary.

    4. **Check for Empty Playlist**: If the 'items' list in the API response is empty, the function returns the playlist table.

    5. **Pagination**: If the 'items' list in the API response is not empty, the script checks for the presence of a 'nextPageToken' in the API response. If a 'nextPageToken' is present, it means that there are more items to retrieve. The script then sends additional requests to the YouTube API to retrieve the remaining items, until all items have been retrieved.

    6. **Return Playlist Data**: After retrieving all items, the function returns the playlist dictionary, which now contains the data for all playlists of the specified YouTube channel.

    7. **Create Playlist Table**: The `pl_table(ch_id)` function is called with the channel ID as an argument to create the playlist table. The function returns a dictionary containing the playlist data for the specified YouTube channel. This dictionary is then assigned to the `playlist_table` variable.

    ## Video Data Extraction

    This script uses the YouTube API to extract data about the videos of specific YouTube playlists. Here's a step-by-step explanation of the process:

    1. **Video Details Function**: The script defines a function to retrieve video data for a specific YouTube video. The function sends a request to the YouTube API, executes the API request, and retrieves the response. If the video exists, the function extracts various pieces of data about the video and stores them in a dictionary.

    2. **Playlist ID and Video ID Function**: The script defines a function to retrieve the video IDs for a specific YouTube playlist. The function sends a request to the YouTube API, executes the API request, and retrieves the response. It then appends the video ID and playlist ID for each item in the API response to a dictionary. If the playlist is not empty, the function also handles pagination by sending additional requests to the YouTube API to retrieve the remaining items.

    3. **Create Playlist DataFrame**: The script creates a pandas DataFrame from the playlist table dictionary and extracts the playlist IDs into a list.

    4. **Get Video IDs**: The script calls the playlist ID and video ID function for each playlist ID to retrieve the video IDs for each playlist. It then creates a DataFrame from the resulting dictionaries, explodes the 'video_id' and 'playlist_id' columns, and drops any rows with missing values or duplicate video IDs.

    5. **Create Video ID List**: The script creates a list of video IDs by extracting the 'video_id' column from the DataFrame.

    6. **Get Video Data**: The script calls the video details function for each video ID to retrieve the data for each video. It stores the resulting dictionaries in a dictionary, with keys in the format 'video_id_i' and values as the video data dictionaries.

    ## Video Data Processing

    This script processes the video data retrieved from the YouTube API. Here's a step-by-step explanation of the process:

    1. **Processing Video ID with Playlist ID**: The script calls a function for each playlist ID to retrieve the video IDs for each playlist. The resulting dictionaries are stored in a list.

    2. **Merge Playlist ID with Video Data**: The script creates a DataFrame from the list of dictionaries. It then explodes the 'playlist_id' and 'video_id' columns to create a DataFrame with one row for each video ID. The DataFrame is then reset to have a continuous index.

    3. **Check for Duplicate Values in Video Data**: The script checks for duplicate video IDs in the DataFrame and drops any duplicates.

    4. **Check for Duplicate Values in Playlist and Video Data**: The script creates a DataFrame from the video table dictionary. It then checks for duplicate video IDs in this DataFrame and drops any duplicates.

    5. **Merge Two Datas**: The script merges the two DataFrames on the 'video_id' column. The resulting DataFrame is transposed to have one row for each video ID.

    6. **Convert DataFrame to JSON**: The script converts the DataFrame to a JSON string. The JSON string is then parsed into a Python dictionary.

    ## Comment Data Extraction

    This script uses the YouTube API to extract data about the comments of specific YouTube videos. Here's a step-by-step explanation of the process:

    1. **Comment Data Function**: The script defines a function to retrieve comment data for a specific YouTube video. The function sends a request to the YouTube API, executes the API request, and retrieves the response. If the video has comments, the function extracts various pieces of data about each comment and stores them in a dictionary.

    2. **Create Video ID List**: The script creates a list of video IDs by iterating over a list of video IDs and including only those video IDs for which the video details function returns a non-`None` value.

    3. **Get Comment Data**: The script calls the comment data function for each video ID to retrieve the comment data for each video. It stores the resulting dictionaries in a dictionary, with keys in the format 'comment_i' and values as the comment data dictionaries.

    ## Storing Data in MongoDB-Transforming 

    This script stores the extracted YouTube data in a MongoDB database. Here's a step-by-step explanation of the process:

    1. **Setting up MongoDB Connection**: The script establishes a connection to a MongoDB server running on localhost at port 27017.

    2. **Database Selection**: The script creates a database name by replacing spaces in the channel name with underscores. This database name is used to select a database in MongoDB.

    3. **Existing Collections**: The script retrieves a list of existing collections in the database.

    4. **Storing Channel Data**: If a collection named "channel_data" does not exist in the database, the script creates a new collection with this name and inserts the channel data into it. If the collection already exists, the script replaces the existing data in the collection with the new channel data.

    5. **Storing Playlist Data**: The same process is repeated for the playlist data, using a collection named "playlist_data".

    6. **Storing Video Data**: The same process is repeated for the video data, using a collection named "video_data".

    7. **Storing Comment Data**: The same process is repeated for the comment data, using a collection named "comment_data".

    8. **Closing the MongoDB Connection**: After all data has been stored, the script closes the MongoDB connection.

    9. **Success Message**: If the channel ID and API key are provided, the script displays a success message. If the channel ID or API key is not provided, the script displays a warning message.

## Data Processing and Structuring

This script processes and structures the YouTube data retrieved from MongoDB. Here's a step-by-step explanation of the process:

- **Video Table Creation**: The script defines a function to create a video table from a MongoDB database. The function retrieves the video data, transforms it into a pandas DataFrame, applies various transformations to the data, and returns the video table.

- **Data Structuring**: The script defines a list of database names and initializes empty lists to collect DataFrames for channel data, playlist data, and comment data. It then connects to the MongoDB server, retrieves the data for each database, and appends the data to the respective lists.

- **Channel Table**: The script concatenates the channel data into a single DataFrame and resets and increments the index.

- **Playlist Table**: The script explodes and concatenates the playlist data into a single DataFrame and resets and increments the index.

- **Video Table**: The script calls the video table creation function for each database, concatenates the resulting video data into a single DataFrame, drops any duplicates, and resets and increments the index.

- **Comment Table**: The script explodes and concatenates the comment data into a single DataFrame, converts the 'comment_published_date' into a datetime format, drops any rows with missing values or duplicates, and resets and increments the index.

## Exporting Data to MySQL- Load Data

This script exports the structured YouTube data to a MySQL database. Here's a step-by-step explanation of the process:

- **Establish MySQL Connection**: The script establishes a connection to a MySQL server.

- **Create SQLAlchemy Engine**: The script creates an SQLAlchemy engine that provides a source of connectivity to the 'guvi_youtube_project' database.

- **Export Channel Data**: The script exports the channel data to a table named 'channel_table' in the MySQL database. If the table already exists, the existing data in the table is replaced with the new channel data.

-  **Export Playlist Data**: The same process is repeated for the playlist data, using a table named 'playlist_table'.

- **Export Video Data**: The same process is repeated for the video data, using a table named 'video_table'.

- **Export Comment Data**: The same process is repeated for the comment data, using a table named 'comment_table'.

- **Close MySQL Connection**: After all data has been exported, the script closes the MySQL connection.


# Streamlit App Tabs

This script uses Streamlit to create an interactive app for viewing and exporting YouTube data. Here's a step-by-step explanation of the process:

## Tab Creation:
The script creates three tabs in the Streamlit app: "Channel_Data", "Export to SQL", and "10 SQL Queries".
- ## Channel Data Tab:
    In the "Channel_Data" tab, the script displays a header and a table with the 'channel_name', 'channel_views', and 'subcribercount' columns from the channel data.

- **Export Button**: The script creates a button labeled "Export channel data MySQL". When this button is clicked, the script calls a function to export the channel data to a MySQL database and displays a success message.

- ## Export to SQL Tab: 
    In the "Export to SQL" tab, the script displays a header and a dropdown list of channel names for the user to select from.

- **Channel Selection**: The user can select a channel from the dropdown list. The selected channel name is appended to a list.

- **Export Button**: The script creates a button labeled "Export to MySQL". When this button is clicked, the script retrieves and structures the data for the selected channel, and then exports the data to a MySQL database. A success message is displayed to indicate that the data has been successfully stored in the MySQL database.

- ## SQL Queries Tab:
    In the "10 SQL Queries" tab, the script displays a header.

- **MySQL Query Function**: The script defines a function to execute a SQL query on a MySQL database. The function establishes a connection to the MySQL server, executes the SQL query, retrieves the result, and transforms the result into a pandas DataFrame.

- **SQL Query Selection and Execution**: The script checks the value of the `query` variable which is assigned to the set of questions that is listed in the dropdown list and executes the corresponding SQL query.    

- **Display Result**: The script displays the result of the SQL query in the Streamlit app.

## Author

* **Nandhagopal S** - *Initial work* - Nandha2790

## License

This project is licensed under the Apache-2.0 license - see the LICENSE.md file for details



 


    









    

