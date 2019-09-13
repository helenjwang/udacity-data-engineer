###Introduction:

Goal is to create an Apache Cassandra database which can create queries on song and user activitydata to answer the questions.

###Project Overview:

Using data modeling with Apache Cassandra and complete an ETL pipeline using Python. 

###Datasets:

event_data/2018-11-08-events.csv event_data/2018-11-09-events.csv

###Project Template:

1. process the event_datafile_new.csv dataset to create a denormalized dataset •
2. model the data tables keeping in mind the queries you need to run •	
3. provide queries that I will need to model your data tables for •	
4. load the data into tables you create in Apache Cassandra and run your queries

###Project Steps:

There are two parts in this project and will be introduced seperately:

Modelling your NoSQL Database or Apache Cassandra Database:
1. Design tables to answer the queries outlined in the project template
2. Write Apache Cassandra CREATE KEYSPACE and SET KEYSPACE statements
3. Develop CREATE statement for each of the tables to address each question
4. Load the data with INSERT statement for each of the tables
5. Include IF NOT EXISTS clauses in your CREATE statements to create tables only if the tables do not already exist. We recommend you also include DROP TABLE statement for each table, this way you can run drop and create tables whenever you want to reset your database and test your ETL pipeline
6. Test by running the proper select statements with the correct WHERE clause

Build ETL Pipeline:
1. Implement the logic in section Part I of the notebook template to iterate through each event file in event_data to process and create a new CSV file in Python
2. Make necessary edits to Part II of the notebook template to include Apache Cassandra CREATE and INSERT three statements to load processed records into relevant tables in your data model
3. Test by running three SELECT statements after running the queries on your database
4. Finally, drop the tables and shutdown the cluster

Files:

1. Project_1B_Project_Template.ipynb: This was template file provided to fill in the details and write the python script
2. Project_1B.ipynb: This is the final file provided in which all the queries have been written with importing the files, generating a new csv file and loading all csv files into one. All verifying the results whether all tables had been loaded accordingly as per requirement
3. Event_datafile_new.csv: This is the final combination of all the files which are in the folder event_data
4. Event_Data Folder: Each event file is present separately, so all the files would be combined into one into event_datafile_new.csv