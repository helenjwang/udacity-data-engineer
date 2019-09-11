###Introduction

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app

####Project Description

Apply the knowledge of Spark and Data Lakes to build and ETL pipeline for a Data Lake hosted on Amazon S3

In this task, we have to build an ETL Pipeline that extracts their data from S3 and process them using Spark and then load back into S3 in a set of Fact and Dimension Tables. This will allow their analytics team to continue finding insights in what songs their users are listening. Will have to deploy this Spark process on a Cluster using AWS

###Project Datasets

Song Data Path --> s3://udacity-dend/song_data Log Data Path --> s3://udacity-dend/log_data Log Data JSON Path --> s3://udacity-dend/log_json_path.json

Song Dataset

The first dataset is a subset of real data from the Million Song Dataset(https://labrosa.ee.columbia.edu/millionsong/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example:

song_data/A/B/C/TRABCEI128F424C983.json song_data/A/A/B/TRAABJL12903CDCF1A.json

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

Log Dataset

The second dataset consists of log files in JSON format. The log files in the dataset with are partitioned by year and month. For example:

log_data/2018/11/2018-11-12-events.json log_data/2018/11/2018-11-13-events.json

And below is an example of what a single log file, 2018-11-13-events.json, looks like.

{"artist":"Pavement", "auth":"Logged In", "firstName":"Sylvie", "gender", "F", "itemInSession":0, "lastName":"Cruz", "length":99.16036, "level":"free", "location":"Klamath Falls, OR", "method":"PUT", "page":"NextSong", "registration":"1.541078e+12", "sessionId":345, "song":"Mercy:The Laundromat", "status":200, "ts":1541990258796, "userAgent":"Mozilla/5.0(Macintosh; Intel Mac OS X 10_9_4...)", "userId":10}

###Schema for Song Play Analysis

A Star Schema would be required for optimized queries on song play queries

###Fact Table

songplays - records in event data associated with song plays i.e. records with page NextSong songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

###Dimension Tables

users - users in the app user_id, first_name, last_name, gender, level

songs - songs in music database song_id, title, artist_id, year, duration

artists - artists in music database artist_id, name, location, lattitude, longitude

time - timestamps of records in songplays broken down into specific units start_time, hour, day, week, month, year, weekday

###Spark Process
The ETL job processes the song files then the log files. The song files are listed and iterated over entering relevant information in the artists and the song folders in parquet. The log files are filtered by the NextSong action. The subsequent dataset is then processed to extract the date, time, year etc. fields and records are then appropriately entered into the time, users and songplays folders in parquet for analysis.


###Project Structure
etl.py - The ETL to reads data from S3, processes that data using Spark, and writes them to a new S3
dl.cfg - Configuration file that contains info about AWS credentials
test.ipynb - test file for etl.py