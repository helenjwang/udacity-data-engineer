import psycopg2

/* Create a connection to the database, get a cursor, and set autocommit to true¶ */
try:
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
except psycopg2.Error as e:
    print("Error: Could not make connection to the Postgres database")
    print(e)
try:
    cur = conn.cursor()
except psycopg2.Error as e:
    print("Error: Could not get cursor to the Database")
    print(e)
conn.set_session(autocommit=True)

/* add Create statements for all tables and insert data into the tables */
try:
    cur.execute("CREATE TABLE IF NOT EXISTS transactions2 (transaction_id int, \
                                                           customer_name varchar, cashier_id int, \
                                                           year int);")
except psycopg2.Error as e:
    print("Error: Issue creating table")
    print (e)

try:
    cur.execute("CREATE TABLE IF NOT EXISTS employees (employee_id int, \
                                                       employee_name varchar);")
except psycopg2.Error as e:
    print("Error: Issue creating table")
    print (e)

try:
    cur.execute("CREATE TABLE IF NOT EXISTS albums_sold (album_id int, transaction_id int, \
                                                          album_name varchar);")
except psycopg2.Error as e:
    print("Error: Issue creating table")
    print (e)

try:
    cur.execute("CREATE TABLE IF NOT EXISTS sales (transaction_id int, amount_spent int);")
except psycopg2.Error as e:
    print("Error: Issue creating table")
    print (e)


try:
    cur.execute("INSERT INTO transactions2 (transaction_id, customer_name, cashier_id, year) \
                 VALUES (%s, %s, %s, %s)", \
                 (1, "Amanda", 1, 2000))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

try:
    cur.execute("INSERT INTO transactions2 (transaction_id, customer_name, cashier_id, year) \
                 VALUES (%s, %s, %s, %s)", \
                 (2, "Toby", 1, 2000))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

try:
    cur.execute("INSERT INTO transactions2 (transaction_id, customer_name, cashier_id, year) \
                 VALUES (%s, %s, %s, %s)", \
                 (3, "Max", 2, 2018))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

try:
    cur.execute("INSERT INTO albums_sold (album_id, transaction_id, album_name) \
                 VALUES (%s, %s, %s)", \
                 (1, 1, "Rubber Soul"))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

try:
    cur.execute("INSERT INTO albums_sold (album_id, transaction_id, album_name) \
                 VALUES (%s, %s, %s)", \
                 (2, 1, "Let It Be"))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

try:
    cur.execute("INSERT INTO albums_sold (album_id, transaction_id, album_name) \
                 VALUES (%s, %s, %s)", \
                 (3, 2, "My Generation"))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

try:
    cur.execute("INSERT INTO albums_sold (album_id, transaction_id, album_name) \
                 VALUES (%s, %s, %s)", \
                 (4, 3, "Meet the Beatles"))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

try:
    cur.execute("INSERT INTO albums_sold (album_id, transaction_id, album_name) \
                 VALUES (%s, %s, %s)", \
                 (5, 3, "Help!"))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

try:
    cur.execute("INSERT INTO employees (employee_id, employee_name) \
                 VALUES (%s, %s)", \
                 (1, "Sam"))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

try:
    cur.execute("INSERT INTO employees (employee_id, employee_name) \
                 VALUES (%s, %s)", \
                 (2, "Bob"))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

try:
    cur.execute("INSERT INTO sales (transaction_id, amount_spent) \
                 VALUES (%s, %s)", \
                 (1, 40))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

try:
    cur.execute("INSERT INTO sales (transaction_id, amount_spent) \
                 VALUES (%s, %s)", \
                 (2, 19))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

try:
    cur.execute("INSERT INTO sales (transaction_id, amount_spent) \
                 VALUES (%s, %s)", \
                 (3, 45))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

/*confirm the tables were created with the alter database */

print("Table: transactions2\n")
try:
    cur.execute("SELECT * FROM transactions2;")
except psycopg2.Error as e:
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()

print("\nTable: albums_sold\n")
try:
    cur.execute("SELECT * FROM albums_sold;")
except psycopg2.Error as e:
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()

print("\nTable: employees\n")
try:
    cur.execute("SELECT * FROM employees;")
except psycopg2.Error as e:
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()

print("\nTable: Sales\n")
try:
    cur.execute("SELECT * FROM sales;")
except psycopg2.Error as e:
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()

/* need to perform a 3 way join on the 4 tables we have created */
try:
    cur.execute("SELECT transactions2.transaction_id, customer_name, employees.employee_name, \
                        year, albums_sold.album_name, sales.amount_spent\
                  FROM ((transactions2 JOIN employees ON \
                         transactions2.cashier_id = employees.employee_id) JOIN \
                         albums_sold ON albums_sold.transaction_id=transactions2.transaction_id) JOIN\
                         sales ON transactions2.transaction_id=sales.transaction_id;")


except psycopg2.Error as e:
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()

/* the other way to do with less join */
/*Create all Tables and insert the data -- calculate the amount spent on each transactions */

try:
    cur.execute("CREATE TABLE IF NOT EXISTS transactions (transaction_id int, \
                                                           customer_name varchar, cashier_id int, \
                                                           year int, amount_spent int);")
except psycopg2.Error as e:
    print("Error: Issue creating table")
    print (e)


/*Insert into all tables*/

try:
    cur.execute("INSERT INTO transactions (transaction_id, customer_name, cashier_id, year, amount_spent) \
                 VALUES (%s, %s, %s, %s, %s)", \
                 (1, "Amanda", 1, 2000, 40))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

try:
    cur.execute("INSERT INTO transactions (transaction_id, customer_name, cashier_id, year, amount_spent) \
                 VALUES (%s, %s, %s, %s, %s)", \
                 (2, "Toby", 1, 2000, 19))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

try:
    cur.execute("INSERT INTO transactions (transaction_id, customer_name, cashier_id, year, amount_spent) \
                 VALUES (%s, %s, %s, %s, %s)", \
                 (3, "Max", 2, 2018, 45))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

/* no join needed*/
try:
    cur.execute("SELECT transaction_id, customer_name, amount_spent FROM transactions;")

except psycopg2.Error as e:
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()


/* calculate the sales by cashier */
/*Create all Tables and insert the data -- calculate the amount spent on each transactions */

try:
    cur.execute("CREATE TABLE IF NOT EXISTS cashier_sales (transaction_id int, cashier_name varchar, \
                                                           cashier_id int, amount_spent int);")
except psycopg2.Error as e:
    print("Error: Issue creating table")
    print (e)


/*Insert into all tables*/

try:
    cur.execute("INSERT INTO cashier_sales (transaction_id, cashier_name, cashier_id, amount_spent) \
                 VALUES (%s, %s, %s, %s)", \
                 (1, "Sam", 1, 40 ))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

try:
    cur.execute("INSERT INTO cashier_sales (transaction_id, cashier_name, cashier_id, amount_spent) \
                 VALUES (%s, %s, %s, %s)", \
                 (2, "Sam", 1, 19 ))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

try:
    cur.execute("INSERT INTO cashier_sales (transaction_id, cashier_name, cashier_id, amount_spent) \
                 VALUES (%s, %s, %s, %s)", \
                 (3, "Max", 2, 45))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

/*Insert into all tables*/
try:
    cur.execute("select cashier_name, SUM(amount_spent) FROM cashier_sales GROUP BY cashier_name;")

except psycopg2.Error as e:
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()

/*drop the tables */
try:
    cur.execute("DROP table albums_sold")
except psycopg2.Error as e:
    print("Error: Dropping table")
    print (e)
try:
    cur.execute("DROP table employees")
except psycopg2.Error as e:
    print("Error: Dropping table")
    print (e)
try:
    cur.execute("DROP table transactions")
except psycopg2.Error as e:
    print("Error: Dropping table")
    print (e)
try:
    cur.execute("DROP table transactions2")
except psycopg2.Error as e:
    print("Error: Dropping table")
    print (e)
try:
    cur.execute("DROP table sales")
except psycopg2.Error as e:
    print("Error: Dropping table")
    print (e)
try:
    cur.execute("DROP table cashier_sales")
except psycopg2.Error as e:
    print("Error: Dropping table")
    print (e)

/*close my cursor and connection */
   cur.close()
   conn.close()