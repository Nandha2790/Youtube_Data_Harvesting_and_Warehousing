# Youtube Data Harvesting and Warehousing
## Overview
This project aims to harvest data from YouTube channels using the YouTube API and store it in MongoDB. The harvested data includes channel information, playlist data, video data, and comment data it is stored in MongoDB. Additionally, the project provides functionality to export the data to MySQL for further analysis.

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
- Use the "Export above listed channel data MySQL" tab in the Streamlit app to export the cleaned data from MongoDB to MySQL.
- Specify the MySQL database connection details (host, port, database name, username, password).
- Click the "Export the selected channel to MySQL" button to transfer the selected channel data to MySQL.

## Viewing and Querying Data
- The "Channel Data" tab displays information about the Extracted YouTube channel list.
- The "Export to MySQL" tab displays the list of Extracted youtube channel where user can select a channel and saved its data to MySQL.
- Use the "10 SQL Queries" tab to view the selected queries.

## Author

* **Nandhagopal S** - *Initial work* - Nandha2790

## License

This project is licensed under the Apache-2.0 license - see the LICENSE.md file for details



 


    









    

