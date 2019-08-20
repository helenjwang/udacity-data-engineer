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
Adding "!" at the beginning of a jupyter cell runs a command in a shell, i.e. we are not running python code but we are running the createdb and psql postgresql commmand-line utilities 
 */

!PGPASSWORD=student createdb -h 127.0.0.1 -U student pagila
!PGPASSWORD=student psql -q -h 127.0.0.1 -U student -d pagila -f Data/pagila-schema.sql
!PGPASSWORD=student psql -q -h 127.0.0.1 -U student -d pagila -f Data/pagila-data.sql

-- 1.2 Connect to the newly created db
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

/* STEP2 : Explore the 3NF Schema
2.1 How much? What data sizes are we looking at? */
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

--2.2 When? What time period are we talking about?
%%sql 
select min(payment_date) as start, max(payment_date) as end from payment;
/* 
2.3 Where? Where do events in this database occur?
TODO: Write a query that displays the number of addresses by district in the address table. Limit the table to the top 10 districts. Your results should match the table below.
 */
%%sql
select district, sum(city_id) as n
from address
group by district
order by n desc
limit 10;

/* STEP3: Perform some simple data analysis
Start by connecting to the database by running the cells below. If you are coming back to this exercise, then uncomment and run the first cell to recreate the database. If you recently completed steps 1 and 2, then skip to the second cell.
 */
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
/*  3NF - Entity Relationship Diagram

3.1 Insight 1: Top Grossing Movies
Payments amounts are in table payment
Movies are in table film
They are not directly linked, payment refers to a rental, rental refers to an inventory item and inventory item refers to a film
payment → rental → inventory → film
 */

 %%sql
select film_id, title, release_year, rental_rate, rating  from film limit 5;

--3.1.2 Payments
%%sql
select * from payment limit 5;

--3.1.3 Inventory
%%sql
select * from inventory limit 5;

--3.1.4 Get the movie of every payment
%%sql
SELECT f.title, p.amount, p.payment_date, p.customer_id                                            
FROM payment p
JOIN rental r    ON ( p.rental_id = r.rental_id )
JOIN inventory i ON ( r.inventory_id = i.inventory_id )
JOIN film f ON ( i.film_id = f.film_id)
limit 5;

/* 3.1.5 sum movie rental revenue
Write a query that displays the amount of revenue from each title. Limit the results to the top 10 grossing titles. Your results should match the table below.
 */
%%sql
SELECT f.title, sum(p.amount) as revenue
FROM payment p
JOIN rental r on (p.rental_id = r. rental_id)
JOIN inventory i on (r.inventory_id = i.inventory_id)
JOIN film f on (i.film_id = f.film_id)
limit 5;

/* 3.2 Insight 2: Top grossing cities
Payments amounts are in table payment
Cities are in table cities
payment → customer → address → city */
%%sql
SELECT f.title, sum(p.amount) as revenue
FROM payment P
JOIN rental r on (p.rental_id = r.rental_id)
JOIN inventory i on (r.inventory_id = i.inventory_id)
JOIN film f on (i.film_id = f.film_id)
limit 5;