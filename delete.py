import psycopg2

def delete_tables():
    """ delete tables from the PostgreSQL database"""
    conn = None
    try:
        # read the connection parameters
        # connect to the PostgreSQL server
        conn = psycopg2.connect("dbname='dejabru' user='team1' password='password1'")
        cur = conn.cursor()
        # create table one by one
        cur.execute("""
        DROP TABLE orders CASCADE;
        DROP TABLE order_products CASCADE;
        DROP TABLE products CASCADE;
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
    delete_tables()