import json
import mysql.connector
from kafka import KafkaConsumer

# Kafka Consumer configuration
consumer = KafkaConsumer(
    'stock-prices',  # Kafka topic name
    bootstrap_servers='localhost:9092',  # Kafka broker address
    auto_offset_reset='earliest',  # Start reading at the earliest available message
    enable_auto_commit=True,
    group_id='stock-price-consumer-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# MySQL connection configuration
db_conn = mysql.connector.connect(
    host="localhost",         # MySQL host
    user="root",     # MySQL username
    password="user", # MySQL password
    database="your_database"  # MySQL database name
)

# Create a cursor object
cursor = db_conn.cursor()

# SQL query to insert stock data into the MySQL table
insert_query = """
INSERT INTO stock_prices (symbol, price, timestamp)
VALUES (%s, %s, %s)
"""

# Function to insert data into MySQL
def insert_into_mysql(stock_data):
    try:
        record = (stock_data['symbol'], stock_data['price'], stock_data['timestamp'])
        cursor.execute(insert_query, record)
        db_conn.commit()  # Commit the transaction
        print(f"Inserted into MySQL: {record}")
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        db_conn.rollback()  # Rollback in case of error

# Kafka Consumer loop
try:
    for message in consumer:
        stock_data = message.value  # Get the message value (stock data)
        insert_into_mysql(stock_data)  # Insert data into MySQL
except KeyboardInterrupt:
    print("Stopping consumer...")
finally:
    # Close MySQL connection and Kafka consumer when done
    cursor.close()
    db_conn.close()
    consumer.close()
