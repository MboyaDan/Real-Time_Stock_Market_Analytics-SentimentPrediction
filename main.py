from fastapi import FastAPI
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import requests
import json
import os
import time

app = FastAPI()

# Kafka configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_NAME = "stock_prices"

def create_kafka_producer():
    retries = 5
    delay = 5
    for _ in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            return producer
        except NoBrokersAvailable:
            print(f"Kafka broker not available. Retrying in {delay} seconds...")
            time.sleep(delay)
    raise Exception("Failed to connect to Kafka broker after multiple retries.")

producer = create_kafka_producer()

# Alpha Vantage API Configuration
STOCK_API_URL = "https://www.alphavantage.co/query"
API_KEY = "9IRBDG1S8KI5PKJT"
SYMBOL = "IBM" 

@app.get("/")
def home():
    return {"message": "Real-Time Stock Ingestion Running"}

@app.get("/fetch_stock_data")
def fetch_stock_data():
    """Fetch stock price data from Alpha Vantage and send to Kafka."""
    try:
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": SYMBOL,
            "apikey": API_KEY
        }
        
        response = requests.get(STOCK_API_URL, params=params)
        data = response.json()

        if "Time Series (Daily)" not in data:
            return {"error": "Invalid response from Alpha Vantage", "details": data}

        # Get the latest available date
        latest_date = max(data["Time Series (Daily)"])
        stock_info = {
            "symbol": SYMBOL,
            "date": latest_date,
            "open": float(data["Time Series (Daily)"][latest_date]["1. open"]),
            "high": float(data["Time Series (Daily)"][latest_date]["2. high"]),
            "low": float(data["Time Series (Daily)"][latest_date]["3. low"]),
            "close": float(data["Time Series (Daily)"][latest_date]["4. close"]),
            "volume": int(data["Time Series (Daily)"][latest_date]["5. volume"]),
        }

        # Send data to Kafka
        producer.send(TOPIC_NAME, stock_info)
        producer.flush()  # Ensure the message is sent immediately

        return {"message": "Stock data sent to Kafka", "data": stock_info}

    except requests.exceptions.RequestException as e:
        return {"error": f"Failed to fetch data from Alpha Vantage: {str(e)}"}
    except Exception as e:
        return {"error": f"An unexpected error occurred: {str(e)}"}