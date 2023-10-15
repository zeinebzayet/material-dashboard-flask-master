#!/usr/bin/env python
# coding: utf-8

# # import libraries

# In[1]:


import mysql.connector
import pandas as pd


# # Extract

# In[2]:


# function to extract data
def extract_data(host, user, password, database, table_names):
    # Create a connection
    connection = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )

    # Initialize an empty dictionary to store DataFrames
    extracted_data = {}

    # Loop through the table names and extract data
    for table_name in table_names:
        sql_query = "SELECT * FROM {}".format(table_name)
        df = pd.read_sql_query(sql_query, connection)
        
        # Store the DataFrame in the dictionary
        extracted_data[table_name] = df

        # Print the first few rows of each DataFrame
        print("Data from table '{}':".format(table_name))
        print(df.head())
        print("\n")

    # Close the database connection
    connection.close()

    return extracted_data

# database information
host = "localhost"
user = "root"
password = ""
database = "bike_store"
table_names = ["brands", "categories", "customers","products","stores","stocks","orders", "order_items","staffs"]

extracted_data = extract_data(host, user, password, database, table_names)


# In[3]:


# Convert date columns to datetime
date_columns = ['order_date', 'required_date', 'shipped_date']
for col in date_columns:
    extracted_data['orders'][col] = pd.to_datetime(extracted_data['orders'][col])

# Extract year, month, and day into separate columns
for col in date_columns:
    extracted_data['orders']['{}_year'.format(col)] = extracted_data['orders'][col].dt.year
    extracted_data['orders']['{}_month'.format(col)] = extracted_data['orders'][col].dt.month
    extracted_data['orders']['{}_day'.format(col)] = extracted_data['orders'][col].dt.day

# Drop the original date columns
extracted_data['orders'].drop(columns=date_columns, inplace=True)

# Fill NaN values with 0 and convert to integers
date_columns_with_prefix = ['{}_year'.format(col) for col in date_columns] + ['{}_month'.format(col) for col in date_columns] + ['{}_day'.format(col) for col in date_columns]
extracted_data['orders'][date_columns_with_prefix] = extracted_data['orders'][date_columns_with_prefix].fillna(0).astype(int)

# Drop multiple columns from customers
columns_to_drop = ['email', 'zip_code']
extracted_data['customers'].drop(columns=columns_to_drop, inplace=True)
# Drop multiple columns from stores
columns_to_drop = ['phone', 'email', 'zip_code']
extracted_data['stores'].drop(columns=columns_to_drop, inplace=True)


# Drop multiple columns from orders
columns_to_drop = ['order_status','staff_id']
extracted_data['orders'].drop(columns=columns_to_drop, inplace=True)

# Drop multiple columns from orders_items
columns_to_drop = ['item_id']
extracted_data['order_items'].drop(columns=columns_to_drop, inplace=True)

extracted_data.pop('staffs', None)
print(extracted_data)


# In[4]:


# Database connection
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="dw_bike_store"
)

# Create a cursor object to execute SQL queries
cursor = connection.cursor()
try:
    # Iterate through the tables in extracted_data and insert the data
    for table_name, df in extracted_data.items():
        if not df.empty:
            # Build the INSERT query dynamically
            columns = ', '.join(df.columns)
            placeholders = ', '.join(['%s'] * len(df.columns))
            # Build the INSERT query dynamically with ON DUPLICATE KEY UPDATE
            query = "INSERT INTO {} ({}) VALUES ({}) ON DUPLICATE KEY UPDATE {}".format(
                table_name, columns, placeholders, ', '.join(["{}=VALUES({})".format(col, col) for col in df.columns])
            )

            # Insert the data into the database
            values = [tuple(str(val) for val in row) for row in df.values]

            for i, original_row in enumerate(values):
                try:
                    cursor.execute(query, original_row)
                except mysql.connector.Error as row_err:
                    print("Error in row {}:".format(i + 1))
                    for col, val in zip(df.columns, original_row):
                        print(col, ":", val)
                    print("Error:", row_err)

            connection.commit()

    # Calculate total sales by customer and insert into retail_facts
    total_sale_by_customer_query = """
    INSERT INTO retail_facts (customer_id, total_sale_by_customer)
    SELECT c.customer_id, SUM(oi.list_price) as total_sale_by_customer
    FROM customers AS c
    JOIN orders AS o ON c.customer_id = o.customer_id
    JOIN order_items AS oi ON o.order_id = oi.order_id
    GROUP BY c.customer_id
    ON DUPLICATE KEY UPDATE total_sale_by_customer = VALUES(total_sale_by_customer)
    """

    cursor.execute(total_sale_by_customer_query)

    # Calculate purchase frequency and insert into retail_facts
    purchase_frequency_query = """
    INSERT INTO retail_facts (customer_id, purchase_frequency)
    SELECT c.customer_id, COUNT(o.order_id) as purchase_frequency
    FROM customers AS c
    JOIN orders AS o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id
    ON DUPLICATE KEY UPDATE purchase_frequency = VALUES(purchase_frequency)
    """

    cursor.execute(purchase_frequency_query)

    # Calculate the number of products purchased by customer and insert into retail_facts
    number_products_by_customer_query = """
    INSERT INTO retail_facts (customer_id, number_products_by_customer)
    SELECT c.customer_id, COUNT(DISTINCT oi.product_id) as number_products_by_customer
    FROM customers AS c
    JOIN orders AS o ON c.customer_id = o.customer_id
    JOIN order_items AS oi ON o.order_id = oi.order_id
    GROUP BY c.customer_id
    ON DUPLICATE KEY UPDATE number_products_by_customer = VALUES(number_products_by_customer)
    """

    cursor.execute(number_products_by_customer_query)

    connection.commit()

except mysql.connector.Error as err:
    print(table_name)
    print("Error: {}".format(err))
    connection.rollback()  # Rollback the transaction in case of an error

finally:
    # Close the cursor and connection
    cursor.close()
    connection.close()

