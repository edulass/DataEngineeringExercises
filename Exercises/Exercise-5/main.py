import psycopg2
import csv

def main():

    host = 'postgres'
    database = 'postgres'
    user = 'postgres'
    pas = 'postgres'
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
    
    cur=conn.cursor()
    
    customers='CREATE TABLE customers(customer_id serial PRIMARY KEY, first_name varchar(30), last_name varchar(30),address_1 varchar(80)'
    customers+=',address_2 varchar(50), city varchar(30), state varchar(50),zip_code integer, join_date Date); '
    
    products='CREATE TABLE products(product_id serial PRIMARY KEY, product_code integer , product_description varchar(30) UNIQUE) ;'
    
    transactions='CREATE TABLE transactions(transaction_id varchar(50) PRIMARY KEY, transaction_date Date, product_id Integer,product_code integer,product_description varchar(30)'
    transactions+=',quantity integer, account_id integer, Foreign Key (product_id) References products (product_id)'
    transactions+=',Foreign Key (product_description) References products (product_description),Foreign Key (account_id) References customers (customer_id));'
    
    cur.execute(customers+products+transactions)
    
    conn.commit()
    
    with open("/app/data/accounts.csv",'r') as f:
        reader=csv.reader(f)
        next(reader)
        cur.copy_from(f,'customers',sep=',')
        
    with open("/app/data/products.csv",'r') as f:
        reader=csv.reader(f)
        next(reader)
        cur.copy_from(f,'products',sep=',')
        
    with open("/app/data/transactions.csv",'r') as f:
        reader=csv.reader(f)
        next(reader)
        cur.copy_from(f,'transactions',sep=',')
    
    conn.commit()
    
    cur.execute('SELECT * FROM customers LIMIT 10;')
    rows = cur.fetchall()
    print(rows)

    cur.execute('SELECT * FROM products LIMIT 10;')
    rows = cur.fetchall()
    print(rows)

    cur.execute('SELECT * FROM transactions LIMIT 10;')
    rows = cur.fetchall()
    print(rows)
    
    cur.close()
    conn.close()
    
if __name__ == '__main__':
    main()
