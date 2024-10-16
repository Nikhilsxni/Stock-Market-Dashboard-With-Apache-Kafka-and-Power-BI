import yfinance as yf
from kafka import KafkaProducer
import json
import time

# Set up Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize data to JSON
)

# Function to fetch real-time stock data
def get_stock_price(symbol):
    stock = yf.Ticker(symbol)
    data = stock.history(period="1d", interval="1m")  # 1-minute interval
    if not data.empty:
        latest_data = data.iloc[-1]  # Get the latest price
        return {
            'symbol': symbol,
            'price': latest_data['Close'],
            'timestamp': str(latest_data.name)  # Timestamp
        }
    return None

# Main loop to continuously fetch and send data to Kafka
stock_symbols = ['GOOG', 'AAPL', 'NVDA']

# Infinite loop to fetch and send stock data
while True:
    for symbol in stock_symbols:
        stock_data = get_stock_price(symbol)
        if stock_data:
            # Send stock data to Kafka topic 'stock-prices'
            producer.send('stock-prices', value=stock_data)
            print(f"Sent data to Kafka: {stock_data}")
    # Sleep for 60 seconds before fetching the data again
    time.sleep(60)
