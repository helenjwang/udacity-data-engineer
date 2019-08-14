/* Exercise 1 - Sakila Star Schema & ETL
All the database tables in this demo are based on public database samples and transformations

Sakila is a sample database created by MySql Link
The postgresql version of it is called Pagila Link
The facts and dimension tables design is based on O'Reilly's public dimensional modelling tutorial schema Link
STEP0: Using ipython-sql
Load ipython-sql: %load_ext sql

To execute SQL queries you write one of the following atop of your cell:

%sql
For a one-liner SQL query
You can access a python var using $
%%sql
For a multi-line SQL query
You can NOT access a python var using $
Running a connection string like: postgresql://postgres:postgres@db:5432/pagila connects to the database

STEP1 : Connect to the local database where Pagila is loaded
1.1 Create the pagila db and fill it with data
Adding "!" at the beginning of a jupyter cell runs a command in a shell, i.e. we are not running python code but we are running the createdb and psql postgresql commmand-line utilities */

!PGPASSWORD=student createdb -h 127.0.0.1 -U student pagila
!PGPASSWORD=student psql -q -h 127.0.0.1 -U student -d pagila -f Data/pagila-schema.sql
!PGPASSWORD=student psql -q -h 127.0.0.1 -U student -d pagila -f Data/pagila-data.sql

--1.2 Connect to the newly created db
%load_ext sql
DB_ENDPOINT = "127.0.0.1"
DB = 'pagila'
DB_USER = 'student'
DB_PASSWORD = 'student'
DB_PORT = '5432'

# postgresql://username:password@host:port/database
conn_string = "postgresql://{}:{}@{}:{}/{}" \
                        .format(DB_USER, DB_PASSWORD, DB_ENDPOINT, DB_PORT, DB)

print(conn_string)
%sql $conn_string

--STEP2 : Explore the 3NF Schema
--2.1 How much? What data sizes are we looking at?
nStores = %sql select count(*) from store;
nFilms = %sql select count(*) from film;
nCustomers = %sql select count(*) from customer;
nRentals = %sql select count(*) from rental;
nPayment = %sql select count(*) from payment;
nStaff = %sql select count(*) from staff;
nCity = %sql select count(*) from city;
nCountry = %sql select count(*) from country;

print("nFilms\t\t=", nFilms[0][0])
print("nCustomers\t=", nCustomers[0][0])
print("nRentals\t=", nRentals[0][0])
print("nPayment\t=", nPayment[0][0])
print("nStaff\t\t=", nStaff[0][0])
print("nStores\t\t=", nStores[0][0])
print("nCities\t\t=", nCity[0][0])
print("nCountry\t\t=", nCountry[0][0])

