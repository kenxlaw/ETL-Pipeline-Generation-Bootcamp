#!/usr/bin/python

import psycopg2

def create_tables():
    """ create tables in the PostgreSQL database"""
    conn = None
    try:
        # read the connection parameters
        # connect to the PostgreSQL server
        conn = psycopg2.connect("dbname='dejabru' user='team1' password='password1'")
        cur = conn.cursor()
        # create table one by one
        cur.execute("""
        CREATE TABLE transactions(
            transactions_id INT NOT NULL PRIMARY KEY,
            branch_id INT NOT NULL,
            date DATE NOT NULL,
            time TIMESTAMP NOT NULL,
            customer_name VARCHAR(255) NOT NULL,
            payment_type VARCHAR(255) NOT NULL,
            total_price MONEY NOT NULL
        );

        CREATE TABLE branches(
            branch_id SERIAL NOT NULL PRIMARY KEY,
            branch_name VARCHAR(255) NOT NULL
        );

        CREATE TABLE customers(
            customer_id SERIAL NOT NULL PRIMARY KEY,
            customer_name VARCHAR(255) NOT NULL,
            branch_id INT NOT NULL
        );

        CREATE TABLE products(
            product_id SERIAL NOT NULL PRIMARY KEY,
            product_name VARCHAR(255) NOT NULL,
            product_price MONEY NOT NULL
        );
        
        CREATE TABLE basket(
            order_id SERIAL NOT NULL PRIMARY KEY,
            product_id INT NOT NULL REFERENCES products(product_id),
            quantity INT NOT NULL
        );
        """)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

if __name__ == '__main__':
    create_tables()