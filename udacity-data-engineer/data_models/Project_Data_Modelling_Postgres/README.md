###Introduction

The aim is to create a Postgres Database Schema and ETL pipeline to optimize queries for song play analysis for a startup called Sparkify

###Project Description

1. model data with Postgres and build and ETL pipeline using Python. 
2. define fact and dimension tables for a Star Schema for a specific focus. 
3. ETL pipeline would transfer data from files located in two local directories into these tables in Postgres using Python and SQL

###Schema for Song Play Analysis

Fact Table

1. songplays records in log data associated with song plays

2. Dimension Tables

3. users in the app

4. songs in music database

5. artists in music database

6. time: timestamps of records in songplays broken down into specific units

###Project Design

1. Database Design is good and efficient because of only a few specific join

ETL Design is also simplified have to read json files and parse accordingly to store the tables into specific columns and proper formatting

Database Script

Writing "python create_tables.py" command in terminal, it is easier to create and recreate tables

Jupyter Notebook

etl.ipynb, a Jupyter notebook is given for verifying each command and data as well and then using those statements and copying into etl.py and running it into terminal using "python etl.py" and then running test.ipynb to see whether data has been loaded in all the tables

Relevant Files Provided

test.ipnb displays the first few rows of each table to let you check your database

create_tables.py drops and created your table

etl.ipynb read and processes a single file from song_data and log_data and loads into your tables in Jupyter notebook

etl.ipynb read and processes a single file from song_data and log_data and loads into your tables in ET

sql_queries.py containg all your sql queries and in imported into the last three files above