from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from kafka import KafkaConsumer
import json
import psycopg2
import os
from google.cloud import firestore
from transformers import pipeline
from dotenv import load_dotenv

#  Load environment variables
load_dotenv()

POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("StockMarketProcessing") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
    .getOrCreate()

# 🔗 Connect to PostgreSQL
try:
    pg_conn = psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )
    pg_cursor = pg_conn.cursor()
    print("✅ Connected to PostgreSQL")
except Exception as e:
    print(f"❌ Error connecting to PostgreSQL: {e}")

#  Firestore Client
firestore_db = firestore.Client()

# 🤖 Sentiment Analysis Model
sentiment_pipeline = pipeline("sentiment-analysis")

# Define Schema for Stock Data
stock_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", FloatType(), True)
])

# Define Schema for News Data
news_schema = StructType([
    StructField("headline", StringType(), True),
    StructField("source", StringType(), True)
])

# 🎧 Read from Kafka
stock_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "stock_prices") \
    .option("startingOffsets", "latest") \
    .load()

news_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "stock_news") \
    .option("startingOffsets", "latest") \
    .load()

# 🛠️ Transform Stock Prices
stock_json_df = stock_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), stock_schema).alias("data"))

processed_stock_df = stock_json_df.select("data.*")

# 🛠️ Process Sentiment Analysis for News
news_json_df = news_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), news_schema).alias("data"))

# 🏆 Apply Sentiment Analysis
def analyze_sentiment(text):
    return sentiment_pipeline(text)[0]['label']

analyze_sentiment_udf = udf(analyze_sentiment, StringType())

processed_news_df = news_json_df.select("data.*") \
    .withColumn("sentiment", analyze_sentiment_udf(col("headline")))

# 💾 Store Processed Data in PostgreSQL
def save_to_postgres(batch_df, epoch_id):
    try:
        batch_df.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}") \
            .option("dbtable", "stock_prices") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        print(f"✅ Successfully saved batch {epoch_id} to PostgreSQL")
    except Exception as e:
        print(f"❌ Error saving batch {epoch_id} to PostgreSQL: {e}")

processed_stock_df.writeStream.foreachBatch(save_to_postgres).start()

# 🔥 Store Sentiment Analysis in Firestore
def save_to_firestore(batch_df, epoch_id):
    try:
        for row in batch_df.collect():
            firestore_db.collection("stock_news_sentiment").document(row["headline"]).set({
                "headline": row["headline"],
                "sentiment": row["sentiment"]
            })
        print(f"✅ Successfully saved batch {epoch_id} to Firestore")
    except Exception as e:
        print(f"❌ Error saving batch {epoch_id} to Firestore: {e}")

processed_news_df.writeStream.foreachBatch(save_to_firestore).start()

# 🎯 Start Streaming
spark.streams.awaitAnyTermination()
