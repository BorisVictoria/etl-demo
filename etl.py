import csv
import re
import json
import subprocess

import pymongo
import pymysql

import pandas as pd
import numpy as np

pd.set_option('display.max_columns', None)
'''
    For the exploratory data analysis, use df.info() to see the column headers, df[df.duplicated(keep=False)] to check all duplicates
    and df[df.duplicated(subset=['col1', 'col2', 'coln'], keep=False) to check duplicates against the composite primary key
'''
def load_csv_pipeline(path):
    
    if 'goDailySales' in path:
        df = pd.read_csv(path, sep=';')
        df = df.drop_duplicates()
        df = df.groupby(['Retailer code', 'Product number', 'Order method code', 'Date', 'Unit price', 'Unit sale price'], as_index=False).agg({'Quantity': 'sum'})
        # print(df.info())
        # print(df[df.duplicated(keep=False)])
        # print(df[df.duplicated(subset=['Retailer code', 'Product number', 'Order method code', 'Date', 'Unit price', 'Unit sale price'], keep=False)])
        df['Date'] = pd.to_datetime(df['Date'])
        create_table = '''
            use go_sales;
            drop table if exists go_daily_sales;
            create table if not exists go_daily_sales (
                retailer_code int,
                product_number int,
                order_method_code int,
                date datetime,
                quantity int,
                unit_price double,
                unit_sale_price double,
                primary key(retailer_code, product_number, order_method_code, date, unit_price, unit_sale_price),
                foreign key(retailer_code)
                    references go_retailers(retailer_code),
                foreign key(product_number)
                    references go_products(product_number),
                foreign key(order_method_code)
                    references go_methods(order_method_code)
            );
        '''
        insert_query = '''
            insert into go_daily_sales values (%s, %s, %s, %s, %s, %s, %s)
        '''
        values = [tuple(row) for row in df.to_numpy()]
        
        cursor.execute(create_table)
        cursor.executemany(insert_query, values)
        
    elif 'goMethods' in path:
        df = pd.read_csv(path, sep=';')
        # print(df.info())
        # print(df[df.duplicated(keep=False)])
        # print(df[df.duplicated(subset=['Order method code'], keep=False)])
        create_table = '''
            use go_sales;
            drop table if exists go_methods;
            create table if not exists go_methods (
                order_method_code int,
                order_method_type varchar(128),
                primary key(order_method_code)
            );
        '''
        insert_query = '''
            insert into go_methods values (%s, %s)
        '''
        values = [tuple(row) for row in df.to_numpy()]
        
        cursor.execute(create_table)
        cursor.executemany(insert_query, values)
        
    elif 'goProducts' in path:
        df = pd.read_csv(path, sep=';')
        # print(df.info())
        # print(df[df.duplicated(keep=False)])
        # print(df[df.duplicated(subset=['Product number'], keep=False)])
        create_table = '''
            use go_sales;
            drop table if exists go_products;
            create table if not exists go_products (
                product_number int,
                product_line varchar(128),
                product_type varchar(128),
                product varchar(128),
                product_brand varchar(128),
                product_color varchar(128),
                unit_cost double,
                unit_price double,
                primary key(product_number)
            );
        '''
        insert_query = '''
            insert into go_products values (%s, %s, %s, %s, %s, %s, %s, %s)
        '''
        values = [tuple(row) for row in df.to_numpy()]
        
        cursor.execute(create_table)
        cursor.executemany(insert_query, values)
        
    elif 'goRetailers' in path:
        df = pd.read_csv(path)
        # print(df.info())
        # print(df[df.duplicated(keep=False)])
        # print(df[df.duplicated(subset=['Retailer code'], keep=False)])
        create_table = '''
            use go_sales;
            drop table if exists go_retailers;
            create table if not exists go_retailers (
              retailer_code int,
              retailer_name varchar(128),
              type varchar(128),
              country varchar(128),
              primary key(retailer_code)  
            );
        '''
        insert_query = '''
            insert into go_retailers values (%s, %s, %s, %s)
        '''
        values = [tuple(row) for row in df.to_numpy()]
        
        cursor.execute(create_table)
        cursor.executemany(insert_query, values)
        
    elif 'Consumer-complaints' in path:
        df = pd.read_csv(path, index_col=[0])
        # print(df.info())
        # print(df[df.duplicated(keep=False)])
        # print(df[df.duplicated(subset=['Complaint ID'], keep=False)])
        df['Date received'] = pd.to_datetime(df['Date received'])
        df['Date sent to company'] = pd.to_datetime(df['Date sent to company'])
        df = df.replace({np.nan: None})
        create_table = '''
            use consumer_complaints;
            drop table if exists consumer_complaints;
            create table if not exists consumer_complaints (
                complaint_id int,
                product varchar(128),
                subproduct varchar(128),
                issue varchar(128),
                subissue varchar(128),
                state varchar(128),
                zip_code double,
                date_received datetime,
                date_sent_to_company datetime,
                company varchar(128),
                company_response varchar(128),
                timely_response varchar(128),
                consumer_disputed varchar(128),
                primary key(complaint_id) 
            );
        '''
        insert_query = '''
            insert into consumer_complaints values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        values = [tuple(row) for row in df.to_numpy()]
        
        cursor.execute(create_table)
        cursor.executemany(insert_query, values)
    else:
        print("You missed a csv kekw")
                
    print(f"Succesfully loaded csv: {path}")
    return

'''
   Mongo-sales.json schema
   
   _id: ObjectId
   saleDate: ISODate
   items: [
    {
       name: string
       tags: string[]
       price: Decimal128
       quantity: int
    }
   ] 
   storeLocation: 'string'
   customer: {
       gender: string
       age: int
       email: string
       satisfaction: int
   }
   couponUsed: boolean
   purchaseMethod: string
'''
def load_json_pipeline(path):
    proc = subprocess.run(["mongoimport", 
                          "--db=supplies", 
                          "--collection=supplies",
                          "--file=" + path,
                          "--drop",
                          "--authenticationDatabase=admin",
                          "--uri=" + MONGO_URI], text=True, stdout=subprocess.PIPE)
    print(proc.stdout)    
    create_table = '''
        use supplies;
        drop table if exists supplies_orders;
        create table if not exists supplies_orders (
            id int,
            sale_date datetime,
            store_location varchar(128),
            customer_email varchar(128),
            customer_gender varchar(1),
            customer_age int,
            customer_satisfaction int,
            coupon_used varchar(128),
            purchase_method varchar(128),
            primary key(id)
         );

        drop table if exists supplies_order_items;
        create table if not exists supplies_order_items (
            supplies_orders_items_key int,
            order_id int,
            name varchar(128),
            price double,
            quantity int,
            primary key(supplies_orders_items_key),
            foreign key(order_id)
                references supplies_orders(id)
        );

        drop table if exists supplies_order_item_tags;
        create table if not exists supplies_order_item_tags (
            supplies_order_item_tag_key int,
            supplies_order_items_key int,
            tagname varchar(128),
            primary key(supplies_order_item_tag_key),
            foreign key(supplies_order_items_key)
                references supplies_order_items(supplies_orders_items_key)
        );
    '''
    db = mongo['supplies']
    collection = db['supplies']
    documents = collection.find()

    insert_supplies_orders = '''
       insert into supplies_orders values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''

    insert_supplies_order_items = '''
        insert into supplies_order_items values (%s, %s, %s, %s, %s)
    '''

    insert_supplies_order_item_tags = '''
        insert into supplies_order_item_tags values (%s, %s, %s)
    '''
    
    # Normalize JSON data
    df = pd.json_normalize(list(documents))

    # Create sales DataFrame
    df_sales = df.drop(columns=['items'])
    df_sales.insert(0, 'id', range(1, len(df_sales) + 1))

    # Create items DataFrame, explode items, and normalize JSON directly
    df_items = df[['items', '_id']].explode('items')
    df_normalized = pd.json_normalize(df_items['items'])
    df_normalized['_id'] = df_items['_id'].values 
    df_items = df_normalized
    
    # Merge with sales to get 'order_id'
    df_items = df_items.merge(df_sales[['_id', 'id']], on='_id').rename(columns={'id': 'order_id'})

    # Assign item 'id' and drop '_id'
    df_items.insert(0, 'id', range(1, len(df_items) + 1))
    df_sales = df_sales.drop(columns=['_id'])
    df_items = df_items.drop(columns=['_id'])

    # Handle tags and explode them into their own DataFrame
    df_tags = df_items[['tags', 'id']].explode('tags').rename(columns={'id': 'item_id'})
    df_tags.insert(0, 'id', range(1, len(df_tags) + 1))

    # Drop tags column from items
    df_items = df_items.drop(columns=['tags'])

    # SQL operations
    cursor.execute(create_table)

    df_sales = df_sales[['id', 'saleDate', 'storeLocation', 'customer.email', 'customer.gender',
                         'customer.age', 'customer.satisfaction', 'couponUsed', 'purchaseMethod']]

    df_items = df_items[['id', 'order_id', 'name', 'price', 'quantity']]
    df_tags = df_tags[['id', 'item_id', 'tags']]

    # Execute SQL insertions
    cursor.executemany(insert_supplies_orders, df_sales.to_numpy().tolist())
    cursor.executemany(insert_supplies_order_items, df_items.to_numpy().tolist())
    cursor.executemany(insert_supplies_order_item_tags, df_tags.to_numpy().tolist())
  
    print(f"Succesfully loaded json: {path}")
    return

def load_sql_pipeline(path):

    dump = open(path, 'r')
    stream = dump.read()
    
    for command in stream.split(';'):
        command = command.strip()
        if command:
            cursor.execute(command)
                
    print(f"Succesfully loaded sql: {path}")
    return

files = [
    'lake/HO1Data-Consumer-complaints.csv', 
    'lake/HO1Data-goMethods.csv',
    'lake/HO1Data-goProducts.csv',    
    'lake/HO1Data-goRetailers.csv',
    'lake/HO1Data-goDailySales.csv',     
    'lake/HO1Data-Mongo-sales.json',
    'lake/Sample DB - employees.sql',
]

print('Welcome to the ETL script of all time')

# Connect to databases
MONGO_URI = 'mongodb://root:password@localhost:27017/'
mongo = pymongo.MongoClient(MONGO_URI)
warehouse = pymysql.connect(host='localhost',
                        user='root',
                        password='password',
                        client_flag=pymysql.constants.CLIENT.MULTI_STATEMENTS)

cursor = warehouse.cursor()
# Initial database setup for MySQL data warehouse
table_init = '''
    drop database if exists go_sales;
    create database go_sales;

    drop database if exists employees;
    create database employees;
    
    drop database if exists consumer_complaints;
    create database consumer_complaints; 
    
    drop database if exists supplies;
    create database supplies;
'''

cursor.execute(table_init)

for file in files:
    if re.search(r'.csv', file):
        load_csv_pipeline(file)
    elif re.search(r'.json', file):
        load_json_pipeline(file)
    elif re.search(r'.sql', file):
        load_sql_pipeline(file)
    else:
        print("What in the fuck is that")

warehouse.commit()
cursor.close()
warehouse.close()
