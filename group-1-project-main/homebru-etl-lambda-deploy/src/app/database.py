import psycopg2

def query_multiple(creds, statements: list[str]):
    try:
        conn = psycopg2.connect(f"dbname={creds['db']} user={creds['user']} password={creds['password']} host={creds['host']} port={creds['port']}")
        cur = conn.cursor()
        # Execute statements
        for statement in statements:
            cur.execute(statement)
        # commit the transaction
        conn.commit()
        # close the database communication
        cur.close()
    except Exception as e:
        raise e
    finally:
        if conn is not None:
            conn.close()

def insert_products(creds, products_list):
    # Generate some SQL statements
    print(f"There are {len(products_list)} products")
    statements = []
    
    statements.append('''CREATE TABLE products_temp(product_id CHARACTER VARYING(50) NOT NULL encode lzo,
                         product_name CHARACTER VARYING(50) encode lzo,
                         product_price DOUBLE PRECISION);
                         ''')
    
    for prod in products_list:
        sql = f"INSERT INTO products_temp (product_id, product_name, product_price) VALUES ('{prod['product_id']}','{prod['product_name']}','{prod['product_price']}');"
        statements.append(sql)
    
    for statement in statements:
        print(statement)
        
    delete_statement = '''
        DELETE FROM products_temp
        USING products
        WHERE products.product_id = products_temp.product_id;
    '''
    statements.append(delete_statement)
    
    merge_statement = '''
        INSERT INTO products
        SELECT * FROM products_temp;
        DROP TABLE "public"."products_temp";
    '''
    statements.append(merge_statement)
    
    query_multiple(creds, statements)


def insert_basket(creds, order_products_list):
    # Generate some SQL statements
    print(f"There are {len(order_products_list)} items in basket")
    statements = []
    
    statements.append('''CREATE TABLE basket_temp(order_id CHARACTER VARYING(50) NOT NULL encode lzo,
                         product_id CHARACTER VARYING(50) NOT NULL encode lzo,
                         quantity INTEGER encode az64);
                         ''')
    
    for basket in order_products_list:
        sql = f"INSERT INTO basket_temp (order_id, product_id, quantity) VALUES ('{basket['order_id']}','{basket['product_id']}','{basket['quantity']}');"
        statements.append(sql)
    
    for statement in statements:
        print(statement)
        
    delete_statement = '''
        DELETE FROM basket_temp
        USING basket
        WHERE basket.order_id = basket_temp.order_id 
        AND basket.product_id = basket_temp.product_id 
        AND basket.quantity = basket_temp.quantity;
    '''
    statements.append(delete_statement)
    
    merge_statement = '''
        INSERT INTO basket
        SELECT * FROM basket_temp;
        DROP TABLE "public"."basket_temp";
    '''
    statements.append(merge_statement)
    
    query_multiple(creds, statements)

def insert_transactions(creds, orders_list):
    # Generate some SQL statements
    print(f"There are {len(orders_list)} transactions")
    statements = []
    
    statements.append('''CREATE TABLE transactions_temp(branch_name CHARACTER VARYING(50) NOT NULL encode lzo,
                         order_id CHARACTER VARYING(50) NOT NULL encode lzo,
                         order_time TIMESTAMP without time zone encode az64,
                         total_price DOUBLE PRECISION,
                         payment_method CHARACTER VARYING(50) encode lzo);
                         ''')
    
    for order in orders_list:
        sql = f"INSERT INTO transactions_temp (branch_name, order_id, order_time, total_price, payment_method) VALUES ('{order['branch_name']}','{order['order_id']}','{order['order_time']}','{order['total_price']}','{order['payment_method']}');"
        statements.append(sql)
    
    for statement in statements:
        print(statement)
        
    delete_statement = '''
        DELETE FROM transactions_temp
        USING transactions
        WHERE transactions.order_id = transactions_temp.order_id;
    '''
    statements.append(delete_statement)
    
    merge_statement = '''
        INSERT INTO transactions
        SELECT * FROM transactions_temp;
        DROP TABLE "public"."transactions_temp";
    '''
    statements.append(merge_statement)
    
    query_multiple(creds, statements)

# results = extract_and_transform.transform("chesterfield.csv")
# product_data = results["products_data"]

# print(product_data)
# insert_products("test", product_data)