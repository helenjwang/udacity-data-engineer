Project: Data Lake

###Introduction

1. move Sparkify data warehouse to a data lake. The data is in S3 and in a directory of JSON logs on user activity on the app
2. + directory with JSON metadata on the songs in their app

###Project Description

1. build an ETL Pipeline that extracts their data from S3 
2. process the data using Spark 
3. load back into S3 in a set of Fact and Dimension Tables. 
4. deploy this Spark process on a Cluster using AWS

###Project Datasets

Song Data Path: s3://udacity-dend/song_data Log Data Path --> s3://udacity-dend/log_data Log Data JSON Path --> s3://udacity-dend/log_json_path.json

###Song Dataset

1. Million Song Dataset(https://labrosa.ee.columbia.edu/millionsong/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. 

eg:

song_data/A/B/C/TRABCEI128F424C983.json song_data/A/A/B/TRAABJL12903CDCF1A.json
so a single song file, TRAABJL12903CDCF1A.json, will look like like.
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

2. Log Dataset:
log files in JSON format. The log files in the dataset with are partitioned by year and month. 

eg:
log_data/2018/11/2018-11-12-events.json log_data/2018/11/2018-11-13-events.json
so a single log file, 2018-11-13-events.json, will look like.
{"artist":"Pavement", "auth":"Logged In", "firstName":"Sylvie", "gender", "F", "itemInSession":0, "lastName":"Cruz", "length":99.16036, "level":"free", "location":"Klamath Falls, OR", "method":"PUT", "page":"NextSong", "registration":"1.541078e+12", "sessionId":345, "song":"Mercy:The Laundromat", "status":200, "ts":1541990258796, "userAgent":"Mozilla/5.0(Macintosh; Intel Mac OS X 10_9_4...)", "userId":10}

3. Schema for Song Play Analysis

1.Star Schema would be required for optimized queries on song play queries

2.Fact Table

---songplays - records in event data associated with song plays i.e. records with page NextSong songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

3. Dimension Tables

---users - users in the app user_id, first_name, last_name, gender, level

---songs - songs in music database song_id, title, artist_id, year, duration

---artists - artists in music database artist_id, name, location, lattitude, longitude

---time - timestamps of records in songplays broken down into specific units start_time, hour, day, week, month, year, weekday

###Project Template Introduction


1. etl.py reads data from S3, processes that data using Spark and writes them back to S3
2. dl.cfg contains AWS Credentials
3. README.md provides discussion on your process and decisions

###ETL Pipeline

1.Load the credentials from dl.cfg
2.Load the Data which are in JSON Files(Song Data and Log Data)
3.After loading the JSON Files from S3 4.Use Spark process this JSON files and then generate a set of Fact and Dimension Tables
4.Load back these dimensional process to S3

###Final Instructions

1.Write correct keys in dl.cfg
2.Open Terminal write the command "python etl.py"
3.Should take about 2-4 mins in total