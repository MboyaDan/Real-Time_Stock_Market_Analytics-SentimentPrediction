from fastapi import FastAPI, Query
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
import requests
import json
import os
import time
import praw
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# FastAPI initialization
app = FastAPI()

# Environment Variables
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
NEWS_API_KEY = os.getenv("NEWS_API_KEY")
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT", "stock_market_analytics")

# Kafka Topics
TOPIC_STOCKS = "stock_prices"
TOPIC_NEWS = "stock_news"
TOPIC_REDDIT = "stock_reddit"

# Alpha Vantage API Configuration
STOCK_API_URL = "https://www.alphavantage.co/query"

# Kafka Producer (Singleton)
producer = None

def create_kafka_producer():
    """Create Kafka producer with retries."""
    global producer
    delay = 5
    for _ in range(5):  # Try up to 5 times before failing
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                request_timeout_ms=10000,  # 10s timeout
                retries=5  # Kafka's internal retry
            )
            print("✅ Connected to Kafka successfully")
            return producer
        except NoBrokersAvailable:
            print(f"⚠️ Kafka broker not available. Retrying in {delay} seconds...")
            time.sleep(delay)
            delay = min(delay * 2, 60)  # Exponential backoff
    print("❌ Could not connect to Kafka after multiple attempts.")
    return None

def get_kafka_producer():
    """Ensure Kafka producer is initialized and connected."""
    global producer
    if producer is None:
        producer = create_kafka_producer()
    return producer

# Initialize Reddit API
try:
    reddit = praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent=REDDIT_USER_AGENT
    )
    print("✅ Reddit API initialized")
except Exception as e:
    print(f"❌ Error initializing Reddit API: {e}")
    reddit = None

# Function to send messages to Kafka with retries
def send_to_kafka(topic, message):
    """Send message to Kafka with retry logic."""
    producer = get_kafka_producer()
    if not producer:
        return {"error": "❌ Kafka producer is unavailable"}

    for attempt in range(3):  # Retry sending 3 times
        try:
            producer.send(topic, message)
            producer.flush()
            return {"message": f"✅ Data sent to Kafka topic {topic}"}
        except KafkaError as e:
            print(f"⚠️ Kafka send failed (attempt {attempt+1}/3): {e}")
            time.sleep(2 ** attempt)  # Exponential backoff
    return {"error": "❌ Failed to send data to Kafka after multiple attempts"}

# Fetch stock price data
@app.get("/fetch_stock_data")
def fetch_stock_data(symbol: str = Query("IBM", min_length=1, description="Stock symbol to fetch data for")):
    try:
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "apikey": ALPHA_VANTAGE_API_KEY
        }
        response = requests.get(STOCK_API_URL, params=params)
        data = response.json()

        if "Time Series (Daily)" not in data:
            return {"error": f"Invalid response from Alpha Vantage for {symbol}", "details": data}

        latest_date = max(data["Time Series (Daily)"])
        stock_info = {
            "symbol": symbol,
            "date": latest_date,
            "open": float(data["Time Series (Daily)"][latest_date]["1. open"]),
            "high": float(data["Time Series (Daily)"][latest_date]["2. high"]),
            "low": float(data["Time Series (Daily)"][latest_date]["3. low"]),
            "close": float(data["Time Series (Daily)"][latest_date]["4. close"]),
            "volume": int(data["Time Series (Daily)"][latest_date]["5. volume"]),
        }

        return send_to_kafka(TOPIC_STOCKS, stock_info)

    except requests.exceptions.RequestException as e:
        return {"error": f"❌ Failed to fetch data from Alpha Vantage: {str(e)}"}
    except Exception as e:
        return {"error": f"❌ Unexpected error: {str(e)}"}

# Fetch stock-related news articles
@app.get("/fetch_stock_news")
def fetch_stock_news():
    try:
        url = f"https://newsapi.org/v2/everything?q=stocks&apiKey={NEWS_API_KEY}"
        response = requests.get(url)
        data = response.json()

        if "articles" not in data:
            return {"error": "❌ Invalid response from NewsAPI", "details": data}

        news_list = []
        for article in data["articles"][:5]:  # Get top 5 articles
            news_item = {
                "title": article["title"],
                "source": article["source"]["name"],
                "published_at": article["publishedAt"],
                "url": article["url"]
            }
            news_list.append(news_item)

        for news in news_list:
            send_to_kafka(TOPIC_NEWS, news)

        return {"message": "✅ Stock news sent to Kafka", "news": news_list}

    except requests.exceptions.RequestException as e:
        return {"error": f"❌ Failed to fetch news data: {str(e)}"}
    except Exception as e:
        return {"error": f"❌ Unexpected error: {str(e)}"}

# Fetch top Reddit posts
@app.get("/fetch_reddit_posts")
def fetch_reddit_posts():
    try:
        if reddit is None:
            return {"error": "❌ Reddit API not initialized"}

        subreddits = ["stocks", "wallstreetbets"]
        reddit_posts = []

        for subreddit_name in subreddits:
            subreddit = reddit.subreddit(subreddit_name)
            for post in subreddit.hot(limit=5):  # Get top 5 hot posts
                post_info = {
                    "subreddit": subreddit_name,
                    "title": post.title,
                    "score": post.score,
                    "url": post.url,
                    "created_utc": post.created_utc
                }
                reddit_posts.append(post_info)

        for post in reddit_posts:
            send_to_kafka(TOPIC_REDDIT, post)

        return {"message": "Reddit posts sent to Kafka", "posts": reddit_posts}

    except Exception as e:
        return {"error": f"Unexpected error while fetching Reddit posts: {str(e)}"}

# Health check endpoint
@app.get("/")
def home():
    return {"message": "✅ Real-Time Stock Ingestion API Running"}
